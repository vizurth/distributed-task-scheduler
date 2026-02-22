package handler

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/constants"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/manager"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/service"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Handler обрабатывает gRPC запросы от воркеров
type Handler struct {
	processpb.UnimplementedTaskProcessorServer
	service       service.Service
	producer      queue.Producer
	workerManager *manager.WorkerManager
	taskQueue     chan *models.KafkaTaskMessage
	mu            sync.RWMutex
}

// NewHandler создает новый обработчик процессора задач
func NewHandler(
	service service.Service,
	producer queue.Producer,
	workerManager *manager.WorkerManager,
	taskQueue chan *models.KafkaTaskMessage,
) *Handler {
	h := &Handler{
		service:       service,
		producer:      producer,
		workerManager: workerManager,
		taskQueue:     taskQueue,
	}

	// Запускаем фоновую задачу для удаления мертвых воркеров
	go h.removeDeadWorkersRoutine()

	return h
}

// ProcessTasks обрабатывает двунаправленный stream для взаимодействия с воркерами
func (h *Handler) ProcessTasks(stream grpc.BidiStreamingServer[processpb.WorkerMessage, processpb.TaskAssignment]) error {
	ctx := stream.Context()
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	var workerID string
	tasksAssigned := 0

	// Cleanup при отключении воркера
	defer func() {
		if workerID != "" {
			h.workerManager.UnregisterWorker(workerID)
			metrics.ProcessorActiveWorkers.Set(float64(h.workerManager.GetActiveWorkersCount()))
			log.Info(ctx, "worker disconnected",
				zap.String("worker_id", workerID),
				zap.Int("tasks_assigned", tasksAssigned))
		}
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Info(ctx, "worker stream closed gracefully", zap.String("worker_id", workerID))
				return nil
			}
			log.Error(ctx, "failed to receive message from worker",
				zap.String("worker_id", workerID),
				zap.Error(err))
			return err
		}

		// Первое сообщение от воркера - регистрируем его
		if workerID == "" {
			workerID = msg.GetWorkerId()
			if workerID == "" {
				log.Error(ctx, "worker_id is empty in first message")
				return fmt.Errorf(constants.ErrMsgWorkerIDRequired)
			}

			h.workerManager.RegisterWorker(workerID)
			metrics.ProcessorActiveWorkers.Set(float64(h.workerManager.GetActiveWorkersCount()))
			log.Info(ctx, "worker registered",
				zap.String("worker_id", workerID),
				zap.Int32("available_slots", msg.AvailableSlots))
		}

		// Обновляем heartbeat
		h.workerManager.UpdateHeartbeat(workerID)

		// Обновляем количество доступных слотов
		h.workerManager.UpdateSlots(workerID, msg.AvailableSlots)

		// Обрабатываем результат задачи, если он есть
		if msg.Result != nil {
			log.Info(ctx, "received task result from worker",
				zap.String("worker_id", workerID),
				zap.String("task_id", msg.Result.TaskId),
				zap.Int64("execution_time_ms", msg.Result.ExecutionTimeMs),
				zap.Bool("has_error", msg.Result.Error != ""))

			if err := h.service.UpdateTask(ctx, msg); err != nil {
				log.Error(ctx, "failed to update task",
					zap.String("worker_id", workerID),
					zap.String("task_id", msg.Result.TaskId),
					zap.Error(err))
			}
		}

		// Если у воркера есть доступные слоты, отправляем ему задачи
		if msg.AvailableSlots > 0 {
			tasks := h.getTasksFromQueue(ctx, int(msg.AvailableSlots))

			if len(tasks) > 0 {
				log.Info(ctx, "assigning tasks to worker",
					zap.String("worker_id", workerID),
					zap.Int("task_count", len(tasks)))
			}

			for _, task := range tasks {
				taskToSend := &processpb.TaskAssignment{
					TaskId:   task.TaskID,
					TaskType: task.TaskType,
					Payload:  task.Payload,
					Priority: task.Priority,
				}

				if err := stream.Send(taskToSend); err != nil {
					log.Error(ctx, "failed to send task to worker",
						zap.String("worker_id", workerID),
						zap.String("task_id", task.TaskID),
						zap.Error(err))
					return err
				}

				tasksAssigned++
				metrics.ProcessorTasksDistributed.WithLabelValues(task.TaskType, "assigned").Inc()

				// Обновляем статус задачи на processing
				if err := h.service.UpdateTaskStatus(ctx, task.TaskID, models.TaskStatusProcessing, workerID); err != nil {
					log.Warn(ctx, "failed to update task status to processing",
						zap.String("task_id", task.TaskID),
						zap.String("worker_id", workerID),
						zap.Error(err))
				}

				log.Debug(ctx, "task assigned to worker",
					zap.String("worker_id", workerID),
					zap.String("task_id", task.TaskID),
					zap.String("task_type", task.TaskType),
					zap.Int32("priority", task.Priority))
			}
		}
	}
}

// getTasksFromQueue получает задачи из очереди (не больше count)
func (h *Handler) getTasksFromQueue(ctx context.Context, count int) []*models.KafkaTaskMessage {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	var tasks []*models.KafkaTaskMessage
	for i := 0; i < count; i++ {
		select {
		case task := <-h.taskQueue:
			if task != nil {
				tasks = append(tasks, task)
			}
		default:
			// Очередь пуста
			if len(tasks) > 0 {
				log.Debug(ctx, "queue is empty, returning available tasks",
					zap.Int("requested", count),
					zap.Int("retrieved", len(tasks)))
			}
			return tasks
		}
	}

	return tasks
}

// removeDeadWorkersRoutine периодически удаляет мертвые воркеры
func (h *Handler) removeDeadWorkersRoutine() {
	ticker := time.NewTicker(constants.DeadWorkerCheckInterval)
	defer ticker.Stop()

	log := logger.GetOrCreateLoggerFromCtx(context.Background())

	for range ticker.C {
		beforeCount := h.workerManager.GetActiveWorkersCount()
		h.workerManager.RemoveDeadWorkers()
		afterCount := h.workerManager.GetActiveWorkersCount()

		if beforeCount != afterCount {
			log.Info(context.Background(), "removed dead workers",
				zap.Int("before", beforeCount),
				zap.Int("after", afterCount),
				zap.Int("removed", beforeCount-afterCount))
			metrics.ProcessorActiveWorkers.Set(float64(afterCount))
		}
	}
}
