package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/manager"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Handler struct {
	processpb.UnimplementedTaskProcessorServer
	pool          *pgxpool.Pool
	client        *redis.Client
	producer      *queue.Producer
	workerManager *manager.WorkerManager
	taskQueue     chan *models.KafkaTaskMessage
	mu            sync.RWMutex
}

func NewHandler(pool *pgxpool.Pool, client *redis.Client, producer *queue.Producer, workerManager *manager.WorkerManager, taskQueue chan *models.KafkaTaskMessage) *Handler {
	h := &Handler{
		pool:          pool,
		client:        client,
		producer:      producer,
		workerManager: workerManager,
		taskQueue:     taskQueue,
	}

	go h.removeDeadWorkersRoutine()

	return h
}

func (h *Handler) ProcessTasks(stream grpc.BidiStreamingServer[processpb.WorkerMessage, processpb.TaskAssignment]) error {
	ctx := stream.Context()
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	var workerID string
	tasksAssigned := 0

	defer func() { // cleanup on disconnect
		if workerID != "" {
			h.workerManager.UnregisterWorker(workerID)
			log.Info(ctx, "worker disconnected", zap.String("worker_id", workerID), zap.Int("tasks_assigned", tasksAssigned))
		}
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Info(ctx, "worker stream closed", zap.String("worker_id", workerID))
				return nil
			}
			log.Error(ctx, "failed to receive message from worker", zap.String("worker_id", workerID), zap.Error(err))
			return err
		}

		if workerID == "" {
			workerID = msg.GetWorkerId()
			h.workerManager.RegisterWorker(workerID)
			log.Info(ctx, "worker registred", zap.String("worker_id", workerID))
		}

		h.workerManager.UpdateHeartbeat(workerID)

		h.workerManager.UpdateSlots(workerID, msg.AvailableSlots)

		if msg.Result != nil {
			h.handleTaskResult() // TODO: заглушка на репозиторий
			// TODO: метрики по результатам
		}

		if msg.AvailableSlots > 0 {
			tasks := h.getTasksFromQueue(ctx, int(msg.AvailableSlots))
			for _, task := range tasks {
				taskToSend := &processpb.TaskAssignment{
					TaskId:   task.TaskID,
					TaskType: string(task.TaskType),
					Payload:  task.Payload,
					Priority: task.Priority,
				}
				if err = stream.Send(taskToSend); err != nil {
					log.Error(ctx, "failed to send task to worker", zap.String("worker_id", workerID), zap.Error(err))
					return err
				}
				tasksAssigned++

				h.updateTaskStatus()
			}
		}
	}
}

// TODO: заглушка чтобы потом сделать репозиторий
func (h *Handler) handleTaskResult(ctx context.Context, msg *processpb.WorkerMessage) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	taskID := msg.Result.TaskId

	// Парсим result
	var resultData interface{}
	if err := json.Unmarshal(msg.Result.Result, &resultData); err != nil {
		log.Error(ctx, "failed to unmarshal result", zap.String("task_id", taskID), zap.Error(err))
		return
	}

	// Обнови БД
	execTimeMs := msg.Result.ExecutionTimeMs
	update := &models.TaskUpdate{
		Status:          func() *models.TaskStatus { s := models.TaskStatusCompleted; return &s }(),
		Result:          msg.Result.Result,
		ExecutionTimeMs: &execTimeMs,
		CompletedAt:     func() *time.Time { t := time.Now(); return &t }(),
	}

	_, err := h.pool.Exec(ctx,
		`UPDATE tasks SET status=$1, result=$2, completed_at=$3, execution_time_ms=$4 WHERE id=$5`,
		fmt.Sprint(update.Status),
		update.Result,
		update.CompletedAt.UnixMilli(),
		execTimeMs,
		taskID,
	)
	if err != nil {
		log.Error(ctx, "failed to update task in db", zap.String("task_id", taskID), zap.Error(err))
		return
	}

	// Отправь результат в Kafka
	kafkaResult := &models.KafkaResultMessage{
		TaskID:          taskID,
		WorkerID:        msg.WorkerId,
		Status:          "completed",
		Result:          resultData,
		ExecutionTimeMs: execTimeMs,
	}

	_ = h.producer.SendResult(kafkaResult)

	log.Info(ctx, "task result processed", zap.String("task_id", taskID), zap.Int64("exec_time_ms", execTimeMs))
}
func (h *Handler) updateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string) {
	now := time.Now()
	_, _ = h.pool.Exec(ctx,
		`UPDATE tasks SET status=$1, started_at=$2, worker_id=$3 WHERE id=$4`,
		string(status),
		now.UnixMilli(),
		workerID,
		taskID,
	)
}

func (h *Handler) getTasksFromQueue(ctx context.Context, count int) []*models.KafkaTaskMessage {
	var tasks []*models.KafkaTaskMessage
	for range count {
		select {
		case val := <-h.taskQueue:
			tasks = append(tasks, val)
		default:
			return tasks
		}
	}

	return tasks
}

func (h *Handler) removeDeadWorkersRoutine() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		h.workerManager.RemoveDeadWorkers()
	}
}
