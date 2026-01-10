package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
)

type TaskExecutor struct {
	workerID string
}

func NewTaskExecutor(workerID string) *TaskExecutor {
	return &TaskExecutor{
		workerID: workerID,
	}
}

func (e *TaskExecutor) ExecuteTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	startTime := time.Now()
	defer func() {
		metrics.WorkerProcessingTasks.WithLabelValues(e.workerID).Dec()
	}()

	metrics.WorkerProcessingTasks.WithLabelValues(e.workerID).Inc()

	var result []byte
	var err error

	switch task.TaskType {
	case "email":
		result, err = e.handleEmailTask(ctx, task)
	case "image":
		result, err = e.handleImageTask(ctx, task)
	case "export":
		result, err = e.handleExportTask(ctx, task)
	default:
		err = fmt.Errorf("unknown task type: %s", task.TaskType)
	}

	duration := time.Since(startTime).Seconds()
	metrics.WorkerTaskExecutionDuration.WithLabelValues(e.workerID, task.TaskType).Observe(duration)

	if err != nil {
		metrics.WorkerTasksExecuted.WithLabelValues(e.workerID, task.TaskType, "failed").Inc()
		return nil, err
	}

	metrics.WorkerTasksExecuted.WithLabelValues(e.workerID, task.TaskType, "success").Inc()

	return result, nil
}

func (e *TaskExecutor) handleEmailTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	var emailTaskData struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
		Body    string `json:"body"`
	}
	if err := json.Unmarshal(task.Payload, &emailTaskData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal email task payload: %w", err)
	}

	// TODO: Implement email sending logic here.
	time.Sleep(2 * time.Second)

	result := map[string]interface{}{
		"status":     "sent",
		"message_id": "msg-" + fmt.Sprintf("%d", time.Now().Unix()),
		"recipient":  emailTaskData.To,
		"sent_at":    time.Now().Format(time.RFC3339),
	}

	return json.Marshal(result)
}

func (e *TaskExecutor) handleImageTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	panic("implement me")
}

func (e *TaskExecutor) handleExportTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	panic("implement me")
}
