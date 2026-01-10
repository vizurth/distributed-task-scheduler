package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/worker/executor"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ProcessorClient struct {
	conn     *grpc.ClientConn
	client   processpb.TaskProcessorClient
	workerID string
	log      *logger.Logger
}

type slotManager struct {
	mu              sync.Mutex
	availableSlots  int32
	tasksInProgress int32
}

func NewProcessorClient(ctx context.Context, processorAddr, workerID string) (*ProcessorClient, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)
	conn, err := grpc.DialContext(ctx, processorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to processor: %w", err)
	}

	client := processpb.NewTaskProcessorClient(conn)

	return &ProcessorClient{
		conn:     conn,
		client:   client,
		workerID: workerID,
		log:      log,
	}, nil
}

func (p *ProcessorClient) ProcessTask(ctx context.Context, executor *executor.TaskExecutor) error {
	stream, err := p.client.ProcessTasks(ctx)
	if err != nil {
		return fmt.Errorf("failed to create task processing stream: %w", err)
	}
	defer stream.CloseSend()

	tasksChan := make(chan *processpb.TaskAssignment, 10)

	go p.recieveTasksFromStream(ctx, stream, tasksChan)

	sm := &slotManager{
		availableSlots:  5,
		tasksInProgress: 0,
	}

	for {
		select {
		case <-ctx.Done():
			p.log.Info(ctx, "context done")
			return nil
		case task := <-tasksChan:
			if task == nil {
				p.log.Info(ctx, "task channel closed")
				return nil
			}

			go p.executeTask(ctx, task, executor, stream, sm)
			sm.mu.Lock()
			sm.tasksInProgress++
			sm.availableSlots--
			sm.mu.Unlock()
		case <-time.After(5 * time.Second):
			sm.mu.Lock()
			sm.availableSlots = 5 - sm.tasksInProgress
			availableSlots := sm.availableSlots
			sm.mu.Unlock()

			msg := processpb.WorkerMessage{
				WorkerId:       p.workerID,
				AvailableSlots: availableSlots,
			}
			if err = stream.Send(&msg); err != nil {
				p.log.Error(ctx, "failed to send message", zap.Error(err))
				return err
			}
			p.log.Info(ctx, "sent hearbeat", zap.Int32("available_slots", availableSlots))
		}
	}
}

func (p *ProcessorClient) Close() {
	p.conn.Close()
}

func (p *ProcessorClient) recieveTasksFromStream(ctx context.Context, stream processpb.TaskProcessor_ProcessTasksClient, tasksChan chan *processpb.TaskAssignment) {
	for {
		task, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				p.log.Info(ctx, "stream closed")
				close(tasksChan)
				return
			}
			p.log.Error(ctx, "failed to receive message", zap.Error(err))
			close(tasksChan)
			return
		}

		select {
		case tasksChan <- task:
		case <-ctx.Done():
			close(tasksChan)
			return
		}
	}
}

func (p *ProcessorClient) executeTask(ctx context.Context, task *processpb.TaskAssignment, executor *executor.TaskExecutor, stream processpb.TaskProcessor_ProcessTasksClient, sm *slotManager) {
	startTime := time.Now()

	defer func() {
		sm.mu.Lock()
		sm.tasksInProgress--
		sm.mu.Unlock()
	}()

	p.log.Info(ctx, "executing task", zap.String("task_id", task.TaskId))

	result, err := executor.ExecuteTask(ctx, task)
	execTime := time.Now().Sub(startTime)

	var resultBytes []byte
	var errMsg string

	if err != nil {
		errMsg = err.Error()
		resultBytes = []byte("{}")
		p.log.Error(ctx, "failed to execute task", zap.String("task_id", task.TaskId), zap.Error(err))
	} else {
		resultBytes = result
		p.log.Info(ctx, "successfully executed task", zap.String("task_id", task.TaskId))
	}

	sm.mu.Lock()
	sm.availableSlots++
	availableSlots := sm.availableSlots
	sm.mu.Unlock()

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
	if err = stream.Send(msg); err != nil {
		p.log.Error(ctx, "failed to send message", zap.Error(err))
	}
}
