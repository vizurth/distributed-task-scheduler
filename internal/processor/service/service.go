package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/constants"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/repository"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
)

type serviceImpl struct {
	repo     repository.Repository
	producer queue.Producer
}

func NewService(repo repository.Repository, producer queue.Producer) Service {
	return &serviceImpl{
		repo:     repo,
		producer: producer,
	}
}

// UpdateTask обновляет задачу на основе результата от воркера
func (s *serviceImpl) UpdateTask(ctx context.Context, msg *processpb.WorkerMessage) error {
	log := logger.GetOrCreateLoggerFromCtx(ctx)
	startTime := time.Now()

	if msg == nil || msg.Result == nil {
		metrics.ProcessorTasksDistributed.WithLabelValues("unknown", "failed").Inc()
		return fmt.Errorf("worker message or result is nil")
	}

	taskID := msg.Result.TaskId
	if taskID == "" {
		metrics.ProcessorTasksDistributed.WithLabelValues("unknown", "failed").Inc()
		return fmt.Errorf(constants.ErrMsgTaskIDRequired)
	}

	// Определяем статус задачи
	var status models.TaskStatus
	if msg.Result.Error != "" {
		status = models.TaskStatusFailed
		log.Warn(ctx, "task completed with error",
			zap.String("task_id", taskID),
			zap.String("worker_id", msg.WorkerId),
			zap.String("error", msg.Result.Error))
	} else {
		status = models.TaskStatusCompleted
	}

	// Парсим result
	var resultData interface{}
	if len(msg.Result.Result) > 0 {
		if err := json.Unmarshal(msg.Result.Result, &resultData); err != nil {
			log.Error(ctx, "failed to unmarshal result",
				zap.String("task_id", taskID),
				zap.Error(err))
			// Продолжаем выполнение даже если не удалось распарсить результат
		}
	}

	// Обновляем БД
	execTimeMs := msg.Result.ExecutionTimeMs
	completedAt := time.Now()
	errorMsg := msg.Result.Error

	update := &models.TaskUpdate{
		Status:          &status,
		Result:          msg.Result.Result,
		ExecutionTimeMs: &execTimeMs,
		CompletedAt:     &completedAt,
	}

	if errorMsg != "" {
		update.Error = &errorMsg
	}

	if err := s.repo.UpdateTask(ctx, taskID, update); err != nil {
		log.Error(ctx, "failed to update task in db",
			zap.String("task_id", taskID),
			zap.Error(err))
		metrics.ProcessorTasksDistributed.WithLabelValues("unknown", "failed").Inc()
		return fmt.Errorf("failed to update task in db: %w", err)
	}

	// Отправляем результат в Kafka для дальнейшей обработки (например, webhooks)
	kafkaResult := &models.KafkaResultMessage{
		TaskID:          taskID,
		WorkerID:        msg.WorkerId,
		Status:          string(status),
		Result:          resultData,
		ExecutionTimeMs: execTimeMs,
	}

	if err := s.producer.SendResult(kafkaResult); err != nil {
		log.Warn(ctx, "failed to send result to kafka",
			zap.String("task_id", taskID),
			zap.Error(err))
		// Не возвращаем ошибку, так как задача уже обновлена в БД
	}

	metrics.ProcessorTasksDistributed.WithLabelValues("task", string(status)).Inc()
	metrics.ProcessorTaskAssignmentDuration.WithLabelValues("task").Observe(time.Since(startTime).Seconds())

	log.Info(ctx, "task result processed successfully",
		zap.String("task_id", taskID),
		zap.String("worker_id", msg.WorkerId),
		zap.String("status", string(status)),
		zap.Int64("exec_time_ms", execTimeMs))

	return nil
}

// UpdateTaskStatus обновляет статус задачи
func (s *serviceImpl) UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string) error {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	if taskID == "" {
		return fmt.Errorf(constants.ErrMsgTaskIDRequired)
	}

	if workerID == "" {
		return fmt.Errorf(constants.ErrMsgWorkerIDRequired)
	}

	now := time.Now()
	if err := s.repo.UpdateTaskStatus(ctx, taskID, status, workerID, now); err != nil {
		log.Error(ctx, "failed to update task status",
			zap.String("task_id", taskID),
			zap.String("worker_id", workerID),
			zap.String("status", string(status)),
			zap.Error(err))
		return fmt.Errorf("failed to update task status: %w", err)
	}

	log.Debug(ctx, "task status updated successfully",
		zap.String("task_id", taskID),
		zap.String("worker_id", workerID),
		zap.String("status", string(status)))

	return nil
}
