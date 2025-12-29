package service

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/models"
)

type Service interface {
	// SubmitTask создает новую задачу и отправляет её в очередь
	SubmitTask(ctx context.Context, taskCreate *models.TaskCreate) (*models.Task, error)

	// GetTaskStatus возвращает статус задачи по ID
	GetTaskStatus(ctx context.Context, taskID string) (*models.Task, error)

	// CancelTask отменяет задачу по ID
	CancelTask(ctx context.Context, taskID string) (*models.Task, error)

	// ListTasks возвращает список задач по фильтру
	ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error)
}
