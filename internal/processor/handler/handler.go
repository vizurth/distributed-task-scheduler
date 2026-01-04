package handler

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/manager"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/service"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Handler struct {
	processpb.UnimplementedTaskProcessorServer
	service       service.Service
	producer      queue.Producer
	workerManager *manager.WorkerManager
	taskQueue     chan *models.KafkaTaskMessage
	mu            sync.RWMutex
}

func NewHandler(service service.Service, producer queue.Producer, workerManager *manager.WorkerManager, taskQueue chan *models.KafkaTaskMessage) *Handler {
	h := &Handler{
		service:       service,
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
			_ = h.service.UpdateTask(ctx, msg)
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

				_ = h.service.UpdateTaskStatus(ctx, task.TaskID, models.TaskStatusProcessing, workerID)
			}
		}
	}
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
