package repository

import (
	"context"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/models"
)

type Repository interface {
	UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) error
	UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string, currTime time.Time) error
}
