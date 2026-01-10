package handler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MockService is a mock implementation of the Service interface
type MockService struct {
	mock.Mock
}

func (m *MockService) SubmitTask(ctx context.Context, taskCreate *models.TaskCreate) (*models.Task, error) {
	args := m.Called(ctx, taskCreate)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Task), args.Error(1)
}

func (m *MockService) GetTaskStatus(ctx context.Context, taskID string) (*models.Task, error) {
	args := m.Called(ctx, taskID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Task), args.Error(1)
}

func (m *MockService) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	args := m.Called(ctx, taskID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Task), args.Error(1)
}

func (m *MockService) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*models.Task), args.Error(1)
}

// SubmitTask
func TestSubmitTask_Success(t *testing.T) {
	mockService := new(MockService)
	expectedTask := &models.Task{
		TaskID:    "task-123",
		TaskType:  models.TaskTypeEmail,
		Status:    models.TaskStatusPending,
		Priority:  5,
		Payload:   []byte(`{"to": "test@example.com"}`),
		UserID:    "user-1",
		CreatedAt: time.Now(),
	}

	mockService.On("SubmitTask", mock.Anything, mock.MatchedBy(func(tc *models.TaskCreate) bool {
		return tc.UserID == "user-1" && tc.TaskType == models.TaskTypeEmail
	})).Once().Return(expectedTask, nil)

	handler := NewHandler(mockService)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("user_id", "user-1"))

	req := &taskpb.SubmitTaskRequest{
		TaskType:   "email",
		Payload:    []byte(`{"to": "test@example.com"}`),
		DeadlineMs: 0,
		WebhookUrl: "",
	}

	resp, err := handler.SubmitTask(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "task-123", resp.TaskId)
	assert.Equal(t, "pending", resp.Status)

	mockService.AssertExpectations(t)
}

func TestSubmitTask_EmptyMetaData(t *testing.T) {
	mockService := new(MockService)
	handler := NewHandler(mockService)

	ctx := context.Background()
	req := &taskpb.SubmitTaskRequest{
		TaskType: "email",
		Payload:  []byte(`{"to": example@test.com}`),
	}

	resp, err := handler.SubmitTask(ctx, req)

	assert.Error(t, err)
	assert.Nil(t, resp)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())

	mockService.AssertExpectations(t)
}

func TestSubmitTask_MissingUserID(t *testing.T) {
	mockService := new(MockService)
	handler := NewHandler(mockService)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs())

	req := &taskpb.SubmitTaskRequest{
		TaskType: "email",
		Payload:  []byte(`{"to": example@test.com}`),
	}

	resp, err := handler.SubmitTask(ctx, req)

	assert.Error(t, err)
	assert.Nil(t, resp)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())

	mockService.AssertExpectations(t)
}

func TestSubmitTask_ServiceError(t *testing.T) {
	mockService := new(MockService)

	mockService.On("SubmitTask", mock.Anything, mock.Anything).Return(nil, assert.AnError)
	req := &taskpb.SubmitTaskRequest{
		TaskType: "email",
		Payload:  []byte(`{"to":"test@example.com"}`),
	}

	handler := NewHandler(mockService)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("user_id", "user-123"))

	resp, err := handler.SubmitTask(ctx, req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
	assert.True(t, ok)

	mockService.AssertExpectations(t)
}

// GetTaskStatus
func TestGetTaskStatus_Success(t *testing.T) {
	mockService := new(MockService)

	now := time.Now()

	expectedTask := &models.Task{
		TaskID:    "task-123",
		TaskType:  models.TaskTypeEmail,
		Status:    models.TaskStatusCompleted,
		Priority:  5,
		Payload:   []byte(`{"to":"test@example.com"}`),
		UserID:    "user-1",
		CreatedAt: now,
		Progress:  100,
		Result:    []byte(`{"status":"sent"}`),
	}

	mockService.On("GetTaskStatus", mock.Anything, "task-123").Return(expectedTask, nil)

	handler := NewHandler(mockService)

	req := &taskpb.GetTaskStatusRequest{
		TaskId: "task-123",
	}

	resp, err := handler.GetTaskStatus(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "task-123", resp.TaskId)
	assert.Equal(t, "completed", resp.Status)

	mockService.AssertExpectations(t)
}

func TestGetTaskStatus_NotFound(t *testing.T) {
	mockService := new(MockService)

	mockService.On("GetTaskStatus", mock.Anything, "task-not-in-db").Return(nil, nil)

	handler := NewHandler(mockService)

	req := &taskpb.GetTaskStatusRequest{
		TaskId: "task-not-in-db",
	}

	resp, err := handler.GetTaskStatus(context.Background(), req)

	assert.Nil(t, resp)
	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())

	mockService.AssertExpectations(t)
}

func TestGetTaskStatus_Error(t *testing.T) {
	mockService := new(MockService)

	mockService.On("GetTaskStatus", mock.Anything, "task-123").Return(nil, assert.AnError)

	handler := NewHandler(mockService)

	req := &taskpb.GetTaskStatusRequest{
		TaskId: "task-123",
	}

	_, err := handler.GetTaskStatus(context.Background(), req)

	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())

	mockService.AssertExpectations(t)
}

// CancelTask
func TestCancelTask_Success(t *testing.T) {
	mockService := new(MockService)

	expectedTask := &models.Task{
		TaskID:    "task-123",
		Status:    models.TaskStatusCancelled,
		CreatedAt: time.Now(),
	}

	mockService.On("CancelTask", mock.Anything, "task-123").Return(expectedTask, nil)

	handler := NewHandler(mockService)
	ctx := context.Background()

	req := &taskpb.CancelTaskRequest{
		TaskId: "task-123",
	}

	resp, err := handler.CancelTask(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "task-123", resp.TaskId)
	assert.Equal(t, "cancelled", resp.Status)
	assert.True(t, resp.Success)

	mockService.AssertExpectations(t)
}

func TestCancelTask_NotFound(t *testing.T) {
	mockService := new(MockService)

	mockService.On("CancelTask", mock.Anything, "task-nonexistent").Return(nil, nil)

	handler := NewHandler(mockService)
	ctx := context.Background()

	req := &taskpb.CancelTaskRequest{
		TaskId: "task-nonexistent",
	}

	resp, err := handler.CancelTask(ctx, req)

	assert.Error(t, err)
	assert.Nil(t, resp)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())

	mockService.AssertExpectations(t)
}

func TestCancelTask_Error(t *testing.T) {
	mockService := new(MockService)

	mockService.On("CancelTask", mock.Anything, "task-123").Return(nil, assert.AnError)

	handler := NewHandler(mockService)

	req := &taskpb.CancelTaskRequest{
		TaskId: "task-123",
	}

	_, err := handler.CancelTask(context.Background(), req)

	assert.Error(t, err)

	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())

	mockService.AssertExpectations(t)
}
