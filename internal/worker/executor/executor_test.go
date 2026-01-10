package executor

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
)

func TestExecuteTask_EmailTask_Success(t *testing.T) {
	executor := NewTaskExecutor("test-worker")

	// Создаём email задачу
	emailPayload := map[string]string{
		"to":      "test@example.com",
		"subject": "Test Email",
		"body":    "This is a test email",
	}

	payloadBytes, _ := json.Marshal(emailPayload)

	task := &processpb.TaskAssignment{
		TaskId:   "task-123",
		TaskType: "email",
		Payload:  payloadBytes,
		Priority: 5,
	}

	// Выполняем задачу
	result, err := executor.ExecuteTask(context.Background(), task)

	// Проверяем результат
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result)

	// Проверяем что результат валидный JSON
	var resultData map[string]interface{}
	err = json.Unmarshal(result, &resultData)
	assert.NoError(t, err)
	assert.Equal(t, "sent", resultData["status"])
	assert.NotEmpty(t, resultData["message_id"])
}

func TestExecuteTask_EmailTask_WithValidPayload(t *testing.T) {
	tests := []struct {
		name    string
		to      string
		subject string
		body    string
		wantErr bool
	}{
		{
			name:    "valid email",
			to:      "user@example.com",
			subject: "Hello",
			body:    "Test message",
			wantErr: false,
		},
		{
			name:    "email with long subject",
			to:      "user@example.com",
			subject: "This is a very long subject line that should still work fine in the system",
			body:    "Test message",
			wantErr: false,
		},
		{
			name:    "email with special characters",
			to:      "user+tag@example.com",
			subject: "Subject with special chars: !@#$%",
			body:    "Body with unicode: 🎉 привет",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewTaskExecutor("test-worker")

			emailPayload := map[string]string{
				"to":      tt.to,
				"subject": tt.subject,
				"body":    tt.body,
			}

			payloadBytes, _ := json.Marshal(emailPayload)

			task := &processpb.TaskAssignment{
				TaskId:   "task-" + tt.name,
				TaskType: "email",
				Payload:  payloadBytes,
				Priority: 5,
			}

			result, err := executor.ExecuteTask(context.Background(), task)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, result)

				var resultData map[string]interface{}
				err = json.Unmarshal(result, &resultData)
				assert.NoError(t, err)
				assert.Equal(t, "sent", resultData["status"])
				assert.NotEmpty(t, resultData["message_id"])
			}
		})
	}
}

func TestExecuteTask_UnknownTaskType(t *testing.T) {
	executor := NewTaskExecutor("test-worker")

	task := &processpb.TaskAssignment{
		TaskId:   "task-unknown",
		TaskType: "unknown_type",
		Payload:  []byte(`{}`),
		Priority: 5,
	}

	result, err := executor.ExecuteTask(context.Background(), task)

	// Должна вернуть ошибку для неизвестного типа
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "unknown task type")
}

func TestExecuteTask_InvalidPayload(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		taskType string
		wantErr  bool
	}{
		{
			name:     "empty payload",
			payload:  []byte(`{}`),
			taskType: "email",
			wantErr:  false, // Может быть успех с пустыми данными или ошибка
		},
		{
			name:     "invalid json",
			payload:  []byte(`{invalid json}`),
			taskType: "email",
			wantErr:  true,
		},
		{
			name:     "null payload",
			payload:  nil,
			taskType: "email",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewTaskExecutor("test-worker")

			task := &processpb.TaskAssignment{
				TaskId:   "task-" + tt.name,
				TaskType: tt.taskType,
				Payload:  tt.payload,
				Priority: 5,
			}

			_, err := executor.ExecuteTask(context.Background(), task)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExecuteTask_Priority_NotAffectsResult(t *testing.T) {
	executor := NewTaskExecutor("test-worker")

	emailPayload := map[string]string{
		"to":      "test@example.com",
		"subject": "Test",
		"body":    "Body",
	}
	payloadBytes, _ := json.Marshal(emailPayload)

	priorities := []int32{1, 5, 10}

	var results [][]byte

	for _, priority := range priorities {
		task := &processpb.TaskAssignment{
			TaskId:   "task-priority-" + string(rune(priority)),
			TaskType: "email",
			Payload:  payloadBytes,
			Priority: priority,
		}

		result, err := executor.ExecuteTask(context.Background(), task)
		require.NoError(t, err)
		results = append(results, result)
	}

	// Результаты должны быть одинаковыми независимо от приоритета
	for i := 1; i < len(results); i++ {
		var result1, result2 map[string]interface{}
		json.Unmarshal(results[0], &result1)
		json.Unmarshal(results[i], &result2)

		// Status должен быть одинаков
		assert.Equal(t, result1["status"], result2["status"])
	}
}
