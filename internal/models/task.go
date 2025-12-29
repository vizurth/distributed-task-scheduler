package models

import "time"

// TaskStatus представляет статус задачи
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
	TaskStatusCancelled  TaskStatus = "cancelled"
)

// TaskType представляет тип задачи
type TaskType string

const (
	TaskTypeEmail  TaskType = "email"
	TaskTypeImage  TaskType = "image"
	TaskTypeExport TaskType = "export"
)

// Task представляет задачу в системе
type Task struct {
	TaskID          string
	TaskType        TaskType
	Status          TaskStatus
	Priority        int32
	Payload         []byte
	DeadlineMs      int64
	WebhookURL      string
	UserID          string
	CreatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	Result          []byte
	Error           string
	Progress        int32
	ExecutionTimeMs int64
}

// TaskCreate представляет данные для создания новой задачи
type TaskCreate struct {
	TaskType   TaskType
	Payload    []byte
	Priority   int32
	DeadlineMs int64
	WebhookURL string
	UserID     string
}

// TaskUpdate представляет данные для обновления задачи
type TaskUpdate struct {
	Status          *TaskStatus
	StartedAt       *time.Time
	CompletedAt     *time.Time
	Result          []byte
	Error           *string
	Progress        *int32
	ExecutionTimeMs *int64
}

// TaskFilter представляет фильтры для поиска задач
type TaskFilter struct {
	UserID       string
	Status       TaskStatus
	Limit        int32
	StatusFilter string // "all", "pending", "processing", "completed", "failed", "cancelled"
}

// KafkaTaskMessage представляет сообщение для отправки в Kafka
type KafkaTaskMessage struct {
	TaskID     string `json:"task_id"`
	TaskType   string `json:"task_type"`
	Payload    []byte `json:"payload"`
	Priority   int32  `json:"priority"`
	DeadlineMs int64  `json:"deadline_ms"`
	UserID     string `json:"user_id"`
}

type KafkaResultMessage struct {
	TaskID          string      `json:"task_id"`
	WorkerID        string      `json:"worker_id"`
	Status          string      `json:"status"`
	Result          interface{} `json:"result,omitempty"`
	Error           string      `json:"error,omitempty"`
	ExecutionTimeMs int64       `json:"execution_time_ms"`
}
