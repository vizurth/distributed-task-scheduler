package converters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
)

func TestProtoToTaskCreate(t *testing.T) {
	payload := []byte(`{"to":"test@example.com"}`)
	req := &taskpb.SubmitTaskRequest{
		TaskType:   "email",
		Payload:    payload,
		Priority:   5,
		DeadlineMs: 3600000,
		WebhookUrl: "https://example.com/webhook",
	}

	result := ProtoToTaskCreate(req, "user-1")

	assert.NotNil(t, result)
	assert.Equal(t, models.TaskTypeEmail, result.TaskType)
	assert.Equal(t, payload, result.Payload)
	assert.Equal(t, int32(5), result.Priority)
	assert.Equal(t, int64(3600000), result.DeadlineMs)
	assert.Equal(t, "https://example.com/webhook", result.WebhookURL)
	assert.Equal(t, "user-1", result.UserID)
}

func TestTaskToProtoSubmitResponse(t *testing.T) {
	now := time.Now()
	task := &models.Task{
		TaskID:    "task-123",
		Status:    models.TaskStatusPending,
		CreatedAt: now,
	}

	resp := TaskToProtoSubmitResponse(task)

	assert.NotNil(t, resp)
	assert.Equal(t, "task-123", resp.TaskId)
	assert.Equal(t, "pending", resp.Status)
	assert.NotNil(t, resp.CreatedAt)
}

func TestTaskToProtoStatusResponse(t *testing.T) {
	now := time.Now()
	startedAt := now.Add(1 * time.Second)
	completedAt := now.Add(2 * time.Second)

	task := &models.Task{
		TaskID:          "task-123",
		Status:          models.TaskStatusCompleted,
		Progress:        100,
		Result:          []byte(`{"status":"success"}`),
		Error:           "",
		ExecutionTimeMs: 1000,
		CreatedAt:       now,
		StartedAt:       &startedAt,
		CompletedAt:     &completedAt,
	}

	resp := TaskToProtoStatusResponse(task)

	assert.NotNil(t, resp)
	assert.Equal(t, "task-123", resp.TaskId)
	assert.Equal(t, "completed", resp.Status)
	assert.Equal(t, int32(100), resp.Progress)
	assert.Equal(t, int64(1000), resp.ExecutionTimeMs)
	assert.NotNil(t, resp.StartedAt)
	assert.NotNil(t, resp.CompletedAt)
}

func TestProtoToTaskFilter(t *testing.T) {
	req := &taskpb.ListTasksRequest{
		UserId:       "user-1",
		Limit:        10,
		StatusFilter: "pending",
	}

	filter := ProtoToTaskFilter(req)

	assert.NotNil(t, filter)
	assert.Equal(t, "user-1", filter.UserID)
	assert.Equal(t, int32(10), filter.Limit)
	assert.Equal(t, "pending", filter.StatusFilter)
}

func TestTaskToKafkaMessage(t *testing.T) {
	task := &models.Task{
		TaskID:     "task-123",
		TaskType:   models.TaskTypeEmail,
		Payload:    []byte(`{"to":"test@example.com"}`),
		Priority:   5,
		DeadlineMs: 3600000,
		UserID:     "user-1",
	}

	msg := TaskToKafkaMessage(task)

	assert.NotNil(t, msg)
	assert.Equal(t, "task-123", msg.TaskID)
	assert.Equal(t, "email", msg.TaskType)
	assert.Equal(t, int32(5), msg.Priority)
	assert.Equal(t, int64(3600000), msg.DeadlineMs)
	assert.Equal(t, "user-1", msg.UserID)
}
