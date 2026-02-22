package constants

import "time"

// Task processing constants
const (
	MinTaskPriority = 1
	MaxTaskPriority = 10

	DefaultTaskListLimit = 100
	MaxTaskListLimit     = 1000
)

// Worker constants
const (
	DefaultWorkerSlots = 5
	MaxWorkerSlots     = 20
)

// Timeout constants
const (
	GRPCDefaultTimeout = 30 * time.Second
	GRPCDialTimeout    = 10 * time.Second

	DBQueryTimeout = 5 * time.Second

	RedisDefaultTimeout = 3 * time.Second

	KafkaFlushTimeout = 30 * time.Second
)

// Cache constants
const (
	TaskCacheTTL = 1 * time.Hour

	KafkaDeliveryChannelBuffer = 10000
	TaskQueueChannelBuffer     = 1000
)

// Heartbeat constants
const (
	WorkerHeartbeatInterval = 5 * time.Second
	WorkerDeadTimeout       = 30 * time.Second
	DeadWorkerCheckInterval = 10 * time.Second
)

// gRPC server tuning constants
const (
	GRPCMaxConcurrentStreams  = 100_000
	GRPCMaxHeaderListSize     = 32 * 1024
	GRPCInitialConnWindowSize = 32 * 1024 * 1024
	GRPCInitialWindowSize     = 16 * 1024 * 1024
	GRPCWriteBufferSize       = 64 * 1024
	GRPCReadBufferSize        = 64 * 1024

	GRPCKeepaliveTime    = 15 * time.Second
	GRPCKeepaliveTimeout = 5 * time.Second
	GRPCKeepaliveMinTime = 5 * time.Second
)

// Monitoring intervals
const (
	DBStatsInterval        = 10 * time.Second
	MetricsMonitorInterval = 10 * time.Second
)

// Processor constants
const (
	ProcessorTaskQueueSize = 1024
	CacheUpdateTimeout     = 5 * time.Second
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
