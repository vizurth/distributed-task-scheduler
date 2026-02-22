package constants

import "time"

// Task processing constants
const (
	// Приоритеты задач
	MinTaskPriority = 1
	MaxTaskPriority = 10

	// Лимиты
	DefaultTaskListLimit = 100
	MaxTaskListLimit     = 1000
)

// Worker constants
const (
	// Количество слотов для выполнения задач
	DefaultWorkerSlots = 5
	MaxWorkerSlots     = 20
)

// Timeout constants
const (
	// gRPC таймауты
	GRPCDefaultTimeout = 30 * time.Second
	GRPCDialTimeout    = 10 * time.Second

	// Database таймауты
	DBQueryTimeout = 5 * time.Second

	// Redis таймауты
	RedisDefaultTimeout = 3 * time.Second

	// Kafka таймауты
	KafkaFlushTimeout = 30 * time.Second
)

// Cache constants
const (
	// TTL для кеша
	TaskCacheTTL = 1 * time.Hour

	// Размеры буферов
	KafkaDeliveryChannelBuffer = 10000
	TaskQueueChannelBuffer     = 1000
)

// Heartbeat constants
const (
	// Интервалы heartbeat
	WorkerHeartbeatInterval = 5 * time.Second
	WorkerDeadTimeout       = 30 * time.Second
	DeadWorkerCheckInterval = 10 * time.Second
)

// Error messages
const (
	ErrMsgTaskIDRequired             = "task_id is required"
	ErrMsgUserIDRequired             = "user_id is required"
	ErrMsgTaskTypeRequired           = "task_type is required"
	ErrMsgPayloadRequired            = "payload is required"
	ErrMsgInvalidPriority            = "priority must be between 1 and 10"
	ErrMsgInvalidTaskType            = "invalid task_type"
	ErrMsgWorkerIDRequired           = "worker_id is required"
	ErrMsgTaskNotFound               = "task not found"
	ErrMsgTaskAlreadyInTerminalState = "task is already in terminal state"
)
