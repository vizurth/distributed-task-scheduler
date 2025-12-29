package service

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/api/converters"
	"github.com/vizurth/distributed-task-scheduler/internal/api/repository"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
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
	submitTask, err := s.repo.CreateTask(ctx, taskCreate)
	if err != nil {
		return nil, err
	}

	kafkaMsg := converters.TaskToKafkaMessage(submitTask)

	err = s.producer.SendTask(kafkaMsg)
	if err != nil {
		return nil, err
	}

	return submitTask, nil
}

// GetTaskStatus возвращает статус задачи по ID
func (s *serviceImpl) GetTaskStatus(ctx context.Context, taskID string) (*models.Task, error) {
	task, err := s.repo.GetTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	return task, nil
}

// CancelTask отменяет задачу по ID
func (s *serviceImpl) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	task, err := s.repo.CancelTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	return task, nil
}

// ListTasks возвращает список задач по фильтру
func (s *serviceImpl) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
	tasks, err := s.repo.ListTasks(ctx, filter)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}
