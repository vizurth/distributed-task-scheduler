package service

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/api/converters"
	"github.com/vizurth/distributed-task-scheduler/internal/api/repository"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	"go.uber.org/zap"
)

type serviceImpl struct {
	repo     repository.Repository
	producer *queue.Producer
}

func NewService(repo repository.Repository, producer *queue.Producer) Service {
	return &serviceImpl{
		repo:     repo,
		producer: producer,
	}
}

// SubmitTask создает новую задачу и отправляет её в очередь
func (s *serviceImpl) SubmitTask(ctx context.Context, taskCreate *models.TaskCreate) (*models.Task, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	submitTask, err := s.repo.CreateTask(ctx, taskCreate)
	if err != nil {
		log.Error(ctx, "failed to create task in repository", zap.String("user_id", taskCreate.UserID), zap.Error(err))
		return nil, err
	}

	kafkaMsg := converters.TaskToKafkaMessage(submitTask)

	err = s.producer.SendTask(kafkaMsg)
	if err != nil {
		log.Error(ctx, "failed to send task to Kafka", zap.String("task_id", submitTask.TaskID), zap.Error(err))
		return nil, err
	}

	log.Info(ctx, "task successfully sent to Kafka",
		zap.String("task_id", submitTask.TaskID),
		zap.String("task_type", string(submitTask.TaskType)),
		zap.String("user_id", submitTask.UserID))

	return submitTask, nil
}

// GetTaskStatus возвращает статус задачи по ID
func (s *serviceImpl) GetTaskStatus(ctx context.Context, taskID string) (*models.Task, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	task, err := s.repo.GetTaskByID(ctx, taskID)
	if err != nil {
		log.Error(ctx, "failed to get task status from repository", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}

	return task, nil
}

// CancelTask отменяет задачу по ID
func (s *serviceImpl) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	task, err := s.repo.CancelTask(ctx, taskID)
	if err != nil {
		log.Error(ctx, "failed to cancel task in repository", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}

	log.Info(ctx, "task successfully cancelled", zap.String("task_id", taskID))

	return task, nil
}

// ListTasks возвращает список задач по фильтру
func (s *serviceImpl) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	tasks, err := s.repo.ListTasks(ctx, filter)
	if err != nil {
		log.Error(ctx, "failed to list tasks from repository",
			zap.String("user_id", filter.UserID),
			zap.String("status_filter", filter.StatusFilter),
			zap.Error(err))
		return nil, err
	}

	log.Info(ctx, "tasks successfully retrieved",
		zap.String("user_id", filter.UserID),
		zap.String("status_filter", filter.StatusFilter),
		zap.Int("count", len(tasks)))

	return tasks, nil
}
