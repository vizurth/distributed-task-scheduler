package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/logger"
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

func (s *serviceImpl) UpdateTask(ctx context.Context, msg *processpb.WorkerMessage) {
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
	if err := s.repo.UpdateTask(ctx, taskID, update); err != nil {
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

	_ = s.producer.SendResult(kafkaResult)

	log.Info(ctx, "task result processed", zap.String("task_id", taskID), zap.Int64("exec_time_ms", execTimeMs))
}
func (s *serviceImpl) UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)
	now := time.Now()
	if err := s.repo.UpdateTaskStatus(ctx, taskID, status, workerID, now); err != nil {
		log.Error(ctx, "failed to update task status", zap.String("task_id", taskID), zap.Error(err))
		return
	}
}
