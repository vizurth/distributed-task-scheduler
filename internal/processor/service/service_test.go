package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
)

type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) error {
	args := m.Called(ctx, taskID, update)
	return args.Error(0)
}
func (m *MockRepository) UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string, currTime time.Time) error {
	args := m.Called(ctx, taskID, status, workerID, currTime)
	return args.Error(0)
}

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) SendTask(msg *models.KafkaTaskMessage) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockProducer) SendResult(msg *models.KafkaResultMessage) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockProducer) Close() {
	m.Called()
}

func TestUpdateTask_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	taskID := "task-123"
	msg := &processpb.WorkerMessage{
		WorkerId: "worker-1",
		Result: &processpb.TaskResult{
			TaskId:          taskID,
			Result:          []byte(`{"status": "success"}`),
			Error:           "",
			ExecutionTimeMs: 500,
		},
	}

	mockRepo.On("UpdateTask", mock.Anything, "task-123", mock.MatchedBy(func(update *models.TaskUpdate) bool {
		return update.Status != nil && *update.Status == models.TaskStatusCompleted
	})).Return(nil)

	mockProducer.On("SendResult", mock.MatchedBy(func(msg *models.KafkaResultMessage) bool {
		return msg.TaskID == "task-123" && msg.Status == "completed"
	})).Return(nil)

	service := NewService(mockRepo, mockProducer)

	err := service.UpdateTask(context.Background(), msg)

	assert.NoError(t, err)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

func TestUpdateTask_RepoError(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	taskID := "task-123"
	msg := &processpb.WorkerMessage{
		WorkerId: "worker-1",
		Result: &processpb.TaskResult{
			TaskId:          taskID,
			Result:          []byte(`{"status": "success"}`),
			Error:           "",
			ExecutionTimeMs: 500,
		},
	}

	mockRepo.On("UpdateTask", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database error"))

	service := NewService(mockRepo, mockProducer)

	err := service.UpdateTask(context.Background(), msg)

	assert.Error(t, err)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertNotCalled(t, "SendTask")
}

func TestUpdateTask_UnCurrJson(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	taskID := "task-123"
	msg := &processpb.WorkerMessage{
		WorkerId: "worker-1",
		Result: &processpb.TaskResult{
			TaskId:          taskID,
			Result:          []byte(`status`),
			Error:           "",
			ExecutionTimeMs: 500,
		},
	}

	service := NewService(mockRepo, mockProducer)

	err := service.UpdateTask(context.Background(), msg)

	assert.Error(t, err)

	mockRepo.AssertNotCalled(t, "UpdateTask")
	mockProducer.AssertNotCalled(t, "SendTask")
}

func TestUpdateTaskStatus_Success(t *testing.T) {
	tests := []struct {
		name        string
		taskID      string
		status      models.TaskStatus
		workerID    string
		shouldError bool
	}{
		{
			name:        "update to processing",
			taskID:      "task-123",
			status:      models.TaskStatusProcessing,
			workerID:    "worker-1",
			shouldError: false,
		},
		{
			name:        "update with different worker",
			taskID:      "task-456",
			status:      models.TaskStatusProcessing,
			workerID:    "worker-99",
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := new(MockRepository)
			mockProducer := new(MockProducer)

			mockRepo.On("UpdateTaskStatus", mock.Anything, tt.taskID, tt.status, tt.workerID, mock.Anything).
				Return(nil)

			svc := NewService(mockRepo, mockProducer)
			ctx := context.Background()

			err := svc.UpdateTaskStatus(ctx, tt.taskID, tt.status, tt.workerID)

			assert.Equal(t, tt.shouldError, err != nil)

			mockRepo.AssertCalled(t, "UpdateTaskStatus", mock.Anything, tt.taskID, tt.status, tt.workerID, mock.Anything)
		})
	}
}

func TestUpdateTaskStatus_RepositoryError(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	mockRepo.On("UpdateTaskStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("database error"))

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	err := svc.UpdateTaskStatus(ctx, "task-123", models.TaskStatusProcessing, "worker-1")

	assert.Error(t, err)

	mockRepo.AssertExpectations(t)
}
