package service

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/models"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
)

type Service interface {
	UpdateTask(ctx context.Context, msg *processpb.WorkerMessage)
	UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string)
}
