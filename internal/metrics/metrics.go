package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// gRPC метрики
	GRPCRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "grpc_requests_total",
			Help:      "Total number of gRPC requests",
		},
		[]string{"method", "status"},
	)

	GRPCRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "grpc_request_duration_seconds",
			Help:      "gRPC request duration in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"method"},
	)

	GRPCRequestErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "grpc_request_errors_total",
			Help:      "Total number of gRPC request errors",
		},
		[]string{"method", "error_type"},
	)

	// Service метрики
	ServiceOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "service_operation_duration_seconds",
			Help:      "Service operation duration in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"operation"},
	)

	ServiceOperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "service_operation_total",
			Help:      "Total number of service operations",
		},
		[]string{"operation", "status"},
	)

	// Database метрики
	DBOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "db_operation_duration_seconds",
			Help:      "Database operation duration in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"operation"},
	)

	DBOperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "db_operation_total",
			Help:      "Total number of database operations",
		},
		[]string{"operation", "status"},
	)

	DBConnectionsAvailable = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "db_connections_available",
			Help:      "Number of available database connections",
		},
	)

	DBConnectionsInUse = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "db_connections_in_use",
			Help:      "Number of database connections in use",
		},
	)

	// Redis метрики
	RedisOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "redis_operation_duration_seconds",
			Help:      "Redis operation duration in seconds",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1},
		},
		[]string{"operation"},
	)

	RedisOperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "redis_operation_total",
			Help:      "Total number of Redis operations",
		},
		[]string{"operation", "status"},
	)

	RedisCacheHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "redis_cache_hits_total",
			Help:      "Total number of Redis cache hits",
		},
	)

	RedisCacheMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "redis_cache_misses_total",
			Help:      "Total number of Redis cache misses",
		},
	)

	// Kafka метрики
	KafkaOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "kafka_operation_duration_seconds",
			Help:      "Kafka operation duration in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"operation"},
	)

	KafkaOperationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "kafka_operation_total",
			Help:      "Total number of Kafka operations",
		},
		[]string{"operation", "status"},
	)

	KafkaMessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "kafka_messages_sent_total",
			Help:      "Total number of Kafka messages sent",
		},
		[]string{"topic"},
	)

	// Task метрики
	TasksSubmittedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "tasks_submitted_total",
			Help:      "Total number of tasks submitted",
		},
		[]string{"task_type"},
	)

	TasksProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "tasks_processing_duration_seconds",
			Help:      "Task processing duration in seconds",
			Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"task_type"},
	)

	TasksByStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "tasks_by_status",
			Help:      "Number of tasks by status",
		},
		[]string{"status"},
	)

	// Processor Service метрики
	ProcessorTasksDistributed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "processor_tasks_distributed_total",
			Help:      "Total number of tasks distributed by processor",
		},
		[]string{"task_type", "status"},
	)

	ProcessorTasksInQueue = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "processor_tasks_in_queue",
			Help:      "Number of tasks currently in processor queue",
		},
	)

	ProcessorActiveWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "processor_active_workers",
			Help:      "Number of active workers connected to processor",
		},
	)

	ProcessorTaskAssignmentDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "processor_task_assignment_duration_seconds",
			Help:      "Time to assign a task to a worker",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"task_type"},
	)

	// Worker метрики
	WorkerTasksExecuted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taskscheduler",
			Name:      "worker_tasks_executed_total",
			Help:      "Total number of tasks executed by worker",
		},
		[]string{"worker_id", "task_type", "status"},
	)

	WorkerTaskExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taskscheduler",
			Name:      "worker_task_execution_duration_seconds",
			Help:      "Task execution duration on worker",
			Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 300},
		},
		[]string{"worker_id", "task_type"},
	)

	WorkerStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "worker_status",
			Help:      "Worker status (1=healthy, 0=unhealthy)",
		},
		[]string{"worker_id"},
	)

	WorkerProcessingTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "taskscheduler",
			Name:      "worker_processing_tasks",
			Help:      "Number of tasks currently being processed by worker",
		},
		[]string{"worker_id"},
	)
)

// RegisterMetrics регистрирует все метрики в Prometheus
func RegisterMetrics() error {
	metrics := []prometheus.Collector{
		GRPCRequestsTotal,
		GRPCRequestDuration,
		GRPCRequestErrors,
		ServiceOperationDuration,
		ServiceOperationTotal,
		DBOperationDuration,
		DBOperationTotal,
		DBConnectionsAvailable,
		DBConnectionsInUse,
		RedisOperationDuration,
		RedisOperationTotal,
		RedisCacheHits,
		RedisCacheMisses,
		KafkaOperationDuration,
		KafkaOperationTotal,
		KafkaMessagesSent,
		TasksSubmittedTotal,
		TasksProcessingDuration,
		TasksByStatus,
		ProcessorTasksDistributed,
		ProcessorTasksInQueue,
		ProcessorActiveWorkers,
		ProcessorTaskAssignmentDuration,
		WorkerTasksExecuted,
		WorkerTaskExecutionDuration,
		WorkerStatus,
		WorkerProcessingTasks,
	}

	for _, m := range metrics {
		if err := prometheus.Register(m); err != nil {
			return err
		}
	}
	return nil
}

// StartMetricsServer запускает HTTP сервер для /metrics endpoint
func StartMetricsServer(port string) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			panic(err)
		}
	}()
}
