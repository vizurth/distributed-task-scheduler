package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
)

type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) CreateTask(ctx context.Context, task *models.TaskCreate) (*models.Task, error) {
	args := m.Called(ctx, task)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Task), args.Error(1)
}

// GetTaskByID возвращает задачу по ID
func (m *MockRepository) GetTaskByID(ctx context.Context, taskID string) (*models.Task, error) {
	args := m.Called(ctx, taskID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Task), args.Error(1)
}

// UpdateTask обновляет задачу
func (m *MockRepository) UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) (*models.Task, error) {
	args := m.Called(ctx, taskID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Task), args.Error(1)
}

// ListTasks возвращает список задач по фильтру
func (m *MockRepository) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*models.Task), args.Error(1)
}

// CancelTask обновляет статус задачи на cancelled
func (m *MockRepository) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	args := m.Called(ctx, taskID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*models.Task), args.Error(1)
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

// SubmitTask
func TestSubmitTask_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	taskCreate := &models.TaskCreate{
		TaskType:   models.TaskTypeEmail,
		Payload:    []byte(`{"to":"test@example.com"}`),
		Priority:   5,
		DeadlineMs: 0,
		UserID:     "user-1",
	}

	expectedTask := &models.Task{
		TaskID:   "task-123",
		TaskType: models.TaskTypeEmail,
		Status:   models.TaskStatusPending,
		Priority: 5,
		Payload:  []byte(`{"to":"test@example.com"}`),
		UserID:   "user-1",
	}

	mockRepo.On("CreateTask", mock.Anything, taskCreate).Return(expectedTask, nil)
	mockProducer.On("SendTask", mock.MatchedBy(func(msg *models.KafkaTaskMessage) bool {
		return msg.TaskID == "task-123" && msg.UserID == "user-1"
	})).Return(nil)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	result, err := svc.SubmitTask(ctx, taskCreate)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "task-123", result.TaskID)
	assert.Equal(t, models.TaskStatusPending, result.Status)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

func TestSubmitTask_RepoError(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	taskCreate := &models.TaskCreate{
		TaskType: models.TaskTypeEmail,
		Payload:  []byte(`{"to":"test@example.com"}`),
		UserID:   "user-1",
	}

	mockRepo.On("CreateTask", mock.Anything, taskCreate).Return(nil, assert.AnError)

	service := NewService(mockRepo, mockProducer)

	result, err := service.SubmitTask(context.Background(), taskCreate)

	assert.Error(t, err)
	assert.Nil(t, result)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertNotCalled(t, "SendTask")
}

func TestSubmitTask_ProducerError(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	taskCreate := &models.TaskCreate{
		TaskType:   models.TaskTypeEmail,
		Payload:    []byte(`{"to":"test@example.com"}`),
		Priority:   5,
		DeadlineMs: 0,
		UserID:     "user-1",
	}

	expectedTask := &models.Task{
		TaskID:   "task-123",
		TaskType: models.TaskTypeEmail,
		Status:   models.TaskStatusPending,
		Priority: 5,
		Payload:  []byte(`{"to":"test@example.com"}`),
		UserID:   "user-1",
	}

	mockRepo.On("CreateTask", mock.Anything, taskCreate).Return(expectedTask, nil)
	mockProducer.On("SendTask", mock.Anything).Return(assert.AnError)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	result, err := svc.SubmitTask(ctx, taskCreate)

	assert.Error(t, err)
	assert.Nil(t, result)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

// GetTaskStatus

func TestGetTaskStatus_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	expectedTask := &models.Task{
		TaskID:   "task-123",
		TaskType: models.TaskTypeEmail,
		Status:   models.TaskStatusCompleted,
	}

	mockRepo.On("GetTaskByID", mock.Anything, "task-123").Return(expectedTask, nil)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	result, err := svc.GetTaskStatus(ctx, "task-123")

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "task-123", result.TaskID)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertNotCalled(t, "SendTask")
}
func TestGetTaskStatus_RepoError(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	mockRepo.On("GetTaskByID", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	result, err := svc.GetTaskStatus(ctx, "task-123")

	assert.Error(t, err)
	assert.Nil(t, result)

	mockRepo.AssertExpectations(t)
	mockProducer.AssertNotCalled(t, "SendTask")
}

func TestCancelTask_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	expectedTask := &models.Task{
		TaskID: "task-123",
		Status: models.TaskStatusCancelled,
	}

	mockRepo.On("CancelTask", mock.Anything, "task-123").Return(expectedTask, nil)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	result, err := svc.CancelTask(ctx, "task-123")

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, models.TaskStatusCancelled, result.Status)

	mockRepo.AssertExpectations(t)
}

func TestListTasks_Success(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	filter := &models.TaskFilter{
		UserID:       "user-1",
		StatusFilter: "all",
		Limit:        10,
	}

	expectedTasks := []*models.Task{
		{
			TaskID:   "task-1",
			TaskType: models.TaskTypeEmail,
			Status:   models.TaskStatusPending,
			UserID:   "user-1",
		},
		{
			TaskID:   "task-2",
			TaskType: models.TaskTypeEmail,
			Status:   models.TaskStatusCompleted,
			UserID:   "user-1",
		},
	}

	mockRepo.On("ListTasks", mock.Anything, filter).Return(expectedTasks, nil)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	results, err := svc.ListTasks(ctx, filter)

	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Len(t, results, 2)
	assert.Equal(t, "task-1", results[0].TaskID)
	assert.Equal(t, "task-2", results[1].TaskID)

	mockRepo.AssertExpectations(t)
}

func TestListTasks_Empty(t *testing.T) {
	mockRepo := new(MockRepository)
	mockProducer := new(MockProducer)

	filter := &models.TaskFilter{
		UserID:       "user-nonexistent",
		StatusFilter: "all",
	}

	mockRepo.On("ListTasks", mock.Anything, filter).Return([]*models.Task{}, nil)

	svc := NewService(mockRepo, mockProducer)
	ctx := context.Background()

	results, err := svc.ListTasks(ctx, filter)

	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Len(t, results, 0)

	mockRepo.AssertExpectations(t)
}
