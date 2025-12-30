package repository

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/models"
)

type Repository interface {
	// CreateTask создает новую задачу в БД
	CreateTask(ctx context.Context, task *models.TaskCreate) (*models.Task, error)

	// GetTaskByID возвращает задачу по ID
	GetTaskByID(ctx context.Context, taskID string) (*models.Task, error)

	// UpdateTask обновляет задачу
	UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) (*models.Task, error)

	// ListTasks возвращает список задач по фильтру
	ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error)

	// CancelTask обновляет статус задачи на cancelled
	CancelTask(ctx context.Context, taskID string) (*models.Task, error)
}
