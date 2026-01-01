package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
)

type TaskExecutor struct {
}

func NewTaskExecutor() *TaskExecutor {
	return &TaskExecutor{}
}

func (e *TaskExecutor) ExecuteTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	switch task.TaskType {
	case "email":
		return e.handleEmailTask(ctx, task)
	case "image":
		return e.handleImageTask(ctx, task)
	case "export":
		return e.handleExportTask(ctx, task)
	default:
		return nil, fmt.Errorf("unknown task type: %s", task.TaskType)
	}
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
