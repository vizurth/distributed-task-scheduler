package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/constants"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/worker/executor"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Размер буфера канала задач
	taskChannelBuffer = 10
)

// ProcessorClient управляет взаимодействием с процессором задач
type ProcessorClient struct {
	conn     *grpc.ClientConn
	client   processpb.TaskProcessorClient
	workerID string
	log      *logger.Logger
}

// slotManager управляет слотами для выполнения задач
type slotManager struct {
	mu              sync.Mutex
	availableSlots  int32
	tasksInProgress int32
}

// NewProcessorClient создает новый клиент для подключения к процессору
func NewProcessorClient(ctx context.Context, processorAddr, workerID string) (*ProcessorClient, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Создаем контекст с таймаутом для подключения
	dialCtx, cancel := context.WithTimeout(ctx, constants.GRPCDialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		processorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Ждем установления соединения
	)
	if err != nil {
		log.Error(ctx, "failed to connect to processor",
			zap.String("processor_addr", processorAddr),
			zap.Error(err))
		return nil, fmt.Errorf("failed to connect to processor: %w", err)
	}

	client := processpb.NewTaskProcessorClient(conn)

	log.Info(ctx, "successfully connected to processor",
		zap.String("processor_addr", processorAddr),
		zap.String("worker_id", workerID))

	return &ProcessorClient{
		conn:     conn,
		client:   client,
		workerID: workerID,
		log:      log,
	}, nil
}

// ProcessTask запускает основной цикл обработки задач
func (p *ProcessorClient) ProcessTask(ctx context.Context, executor *executor.TaskExecutor) error {
	stream, err := p.client.ProcessTasks(ctx)
	if err != nil {
		p.log.Error(ctx, "failed to create task processing stream", zap.Error(err))
		return fmt.Errorf("failed to create task processing stream: %w", err)
	}
	defer stream.CloseSend()

	tasksChan := make(chan *processpb.TaskAssignment, taskChannelBuffer)

	// Запускаем горутину для получения задач из стрима
	go p.receiveTasksFromStream(ctx, stream, tasksChan)

	sm := &slotManager{
		availableSlots:  constants.DefaultWorkerSlots,
		tasksInProgress: 0,
	}

	heartbeatTicker := time.NewTicker(constants.WorkerHeartbeatInterval)
	defer heartbeatTicker.Stop()

	p.log.Info(ctx, "worker started processing tasks",
		zap.String("worker_id", p.workerID),
		zap.Int32("max_concurrent_tasks", constants.DefaultWorkerSlots))

	for {
		select {
		case <-ctx.Done():
			p.log.Info(ctx, "context done, stopping worker",
				zap.String("worker_id", p.workerID))
			return ctx.Err()

		case task, ok := <-tasksChan:
			if !ok {
				p.log.Info(ctx, "task channel closed, stopping worker",
					zap.String("worker_id", p.workerID))
				return nil
			}

			go p.executeTask(ctx, task, executor, stream, sm)

			sm.mu.Lock()
			sm.tasksInProgress++
			sm.availableSlots--
			p.log.Debug(ctx, "task assigned",
				zap.String("task_id", task.TaskId),
				zap.Int32("tasks_in_progress", sm.tasksInProgress),
				zap.Int32("available_slots", sm.availableSlots))
			sm.mu.Unlock()

		case <-heartbeatTicker.C:
			sm.mu.Lock()
			sm.availableSlots = constants.DefaultWorkerSlots - sm.tasksInProgress
			availableSlots := sm.availableSlots
			tasksInProgress := sm.tasksInProgress
			sm.mu.Unlock()

			msg := &processpb.WorkerMessage{
				WorkerId:       p.workerID,
				AvailableSlots: availableSlots,
			}

			if err := stream.Send(msg); err != nil {
				p.log.Error(ctx, "failed to send heartbeat",
					zap.String("worker_id", p.workerID),
					zap.Error(err))
				return fmt.Errorf("failed to send heartbeat: %w", err)
			}

			p.log.Debug(ctx, "sent heartbeat",
				zap.String("worker_id", p.workerID),
				zap.Int32("available_slots", availableSlots),
				zap.Int32("tasks_in_progress", tasksInProgress))
		}
	}
}

// Close закрывает соединение с процессором
func (p *ProcessorClient) Close() {
	if p.conn != nil {
		p.log.Info(context.Background(), "closing processor connection",
			zap.String("worker_id", p.workerID))
		p.conn.Close()
	}
}

// receiveTasksFromStream получает задачи из gRPC стрима
func (p *ProcessorClient) receiveTasksFromStream(ctx context.Context, stream processpb.TaskProcessor_ProcessTasksClient, tasksChan chan *processpb.TaskAssignment) {
	defer close(tasksChan)

	for {
		task, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				p.log.Info(ctx, "stream closed by server",
					zap.String("worker_id", p.workerID))
				return
			}
			p.log.Error(ctx, "failed to receive task from stream",
				zap.String("worker_id", p.workerID),
				zap.Error(err))
			return
		}

		select {
		case tasksChan <- task:
			p.log.Debug(ctx, "received task from stream",
				zap.String("worker_id", p.workerID),
				zap.String("task_id", task.TaskId),
				zap.String("task_type", task.TaskType))
		case <-ctx.Done():
			p.log.Info(ctx, "context done, stopping task receiver",
				zap.String("worker_id", p.workerID))
			return
		}
	}
}

// executeTask выполняет задачу и отправляет результат обратно в процессор
func (p *ProcessorClient) executeTask(
	ctx context.Context,
	task *processpb.TaskAssignment,
	executor *executor.TaskExecutor,
	stream processpb.TaskProcessor_ProcessTasksClient,
	sm *slotManager,
) {
	startTime := time.Now()

	defer func() {
		sm.mu.Lock()
		sm.tasksInProgress--
		sm.mu.Unlock()
	}()

	p.log.Info(ctx, "executing task",
		zap.String("worker_id", p.workerID),
		zap.String("task_id", task.TaskId),
		zap.String("task_type", task.TaskType),
		zap.Int32("priority", task.Priority))

	result, err := executor.ExecuteTask(ctx, task)
	execTime := time.Since(startTime)

	var resultBytes []byte
	var errMsg string
	var status string

	if err != nil {
		errMsg = err.Error()
		resultBytes = []byte("{}")
		status = "failed"
		p.log.Error(ctx, "task execution failed",
			zap.String("worker_id", p.workerID),
			zap.String("task_id", task.TaskId),
			zap.String("task_type", task.TaskType),
			zap.Duration("execution_time", execTime),
			zap.Error(err))
	} else {
		resultBytes = result
		status = "completed"
		p.log.Info(ctx, "task executed successfully",
			zap.String("worker_id", p.workerID),
			zap.String("task_id", task.TaskId),
			zap.String("task_type", task.TaskType),
			zap.Duration("execution_time", execTime))
	}

	// Обновляем доступные слоты
	sm.mu.Lock()
	sm.availableSlots++
	availableSlots := sm.availableSlots
	sm.mu.Unlock()

	// Отправляем результат
	msg := &processpb.WorkerMessage{
		WorkerId: p.workerID,
		Result: &processpb.TaskResult{
			TaskId:          task.TaskId,
			Result:          resultBytes,
			Error:           errMsg,
			ExecutionTimeMs: execTime.Milliseconds(),
		},
		AvailableSlots: availableSlots,
	}

	if err := stream.Send(msg); err != nil {
		p.log.Error(ctx, "failed to send task result",
			zap.String("worker_id", p.workerID),
			zap.String("task_id", task.TaskId),
			zap.String("status", status),
			zap.Error(err))
		return
	}

	p.log.Debug(ctx, "task result sent",
		zap.String("worker_id", p.workerID),
		zap.String("task_id", task.TaskId),
		zap.String("status", status),
		zap.Int32("available_slots", availableSlots))
}
