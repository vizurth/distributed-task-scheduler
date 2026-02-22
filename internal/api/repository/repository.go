package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/vizurth/distributed-task-scheduler/internal/constants"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"go.uber.org/zap"
)

const (
	redisTaskKeyPrefix = "task:"
)

type repositoryImpl struct {
	db     *pgxpool.Pool
	psql   sq.StatementBuilderType
	client *redis.Client
}

func NewRepository(db *pgxpool.Pool, client *redis.Client) Repository {
	return &repositoryImpl{
		db:     db,
		psql:   sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		client: client,
	}
}

// getRedisKey возвращает ключ для Redis
func (r *repositoryImpl) getRedisKey(taskID string) string {
	return redisTaskKeyPrefix + taskID
}

// timeToUnixMs конвертирует time.Time в Unix timestamp в миллисекундах
func timeToUnixMs(t time.Time) int64 {
	return t.UnixMilli()
}

// unixMsToTime конвертирует Unix timestamp в миллисекундах в time.Time
func unixMsToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

// CreateTask создает новую задачу в БД
func (r *repositoryImpl) CreateTask(ctx context.Context, task *models.TaskCreate) (*models.Task, error) {
	start := time.Now()
	operation := "CreateTask"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	taskID := uuid.New().String()
	now := time.Now()
	createdAtMs := timeToUnixMs(now)

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Конвертируем payload в JSONB
	payloadJSON, err := json.Marshal(json.RawMessage(task.Payload))
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to marshal payload", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Строим SQL запрос с помощью squirrel
	query, args, err := r.psql.
		Insert("tasks").
		Columns("id", "user_id", "task_type", "payload", "status", "priority", "deadline_ms", "created_at", "webhook_url").
		Values(taskID, task.UserID, string(task.TaskType), payloadJSON, string(models.TaskStatusPending), task.Priority, task.DeadlineMs, createdAtMs, task.WebhookURL).
		ToSql()

	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to build insert query", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to build insert query: %w", err)
	}

	_, err = r.db.Exec(ctx, query, args...)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to insert task into database", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to insert task: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()

	// Создаем объект Task для возврата
	createdTask := &models.Task{
		TaskID:     taskID,
		TaskType:   task.TaskType,
		Status:     models.TaskStatusPending,
		Priority:   task.Priority,
		Payload:    task.Payload,
		DeadlineMs: task.DeadlineMs,
		WebhookURL: task.WebhookURL,
		UserID:     task.UserID,
		CreatedAt:  now,
		Progress:   0,
	}

	// Кешируем задачу в Redis
	if err := r.cacheTask(ctx, createdTask); err != nil {
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Warn(ctx, "failed to cache task in Redis", zap.String("task_id", taskID), zap.Error(err))
	}

	return createdTask, nil
}

// GetTaskByID возвращает задачу по ID
func (r *repositoryImpl) GetTaskByID(ctx context.Context, taskID string) (*models.Task, error) {
	start := time.Now()
	operation := "GetTaskByID"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	// Сначала проверяем Redis кеш
	cachedTask, err := r.getTaskFromCache(ctx, taskID)
	if err == nil && cachedTask != nil {
		metrics.RedisCacheHits.Inc()
		metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
		return cachedTask, nil
	}

	// Cache miss - будем получать из БД
	metrics.RedisCacheMisses.Inc()

	// Если в кеше нет, получаем из PostgreSQL
	log := logger.GetOrCreateLoggerFromCtx(ctx)
	query, args, err := r.psql.
		Select("id", "user_id", "task_type", "payload", "result", "error", "status", "priority", "deadline_ms",
			"created_at", "started_at", "completed_at", "execution_time_ms", "webhook_url").
		From("tasks").
		Where(sq.Eq{"id": taskID}).
		ToSql()

	if err != nil {
		log.Error(ctx, "failed to build select query", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to build select query: %w", err)
	}

	var dbTask struct {
		ID              string
		UserID          string
		TaskType        string
		Payload         json.RawMessage
		Result          *json.RawMessage
		Error           *string
		Status          string
		Priority        int32
		DeadlineMs      int64
		CreatedAt       int64
		StartedAt       *int64
		CompletedAt     *int64
		ExecutionTimeMs *int64
		WebhookURL      *string
	}

	err = r.db.QueryRow(ctx, query, args...).Scan(
		&dbTask.ID, &dbTask.UserID, &dbTask.TaskType, &dbTask.Payload, &dbTask.Result,
		&dbTask.Error, &dbTask.Status, &dbTask.Priority, &dbTask.DeadlineMs,
		&dbTask.CreatedAt, &dbTask.StartedAt, &dbTask.CompletedAt, &dbTask.ExecutionTimeMs, &dbTask.WebhookURL,
	)

	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		if err == pgx.ErrNoRows {
			log.Warn(ctx, "task not found in database", zap.String("task_id", taskID))
			return nil, fmt.Errorf("task not found: %w", err)
		}
		log.Error(ctx, "failed to query task from database", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to query task: %w", err)
	}

	// Конвертируем payload из JSONB
	var payloadBytes []byte
	if err := json.Unmarshal(dbTask.Payload, &payloadBytes); err != nil {
		// Если не удалось распарсить как массив байт, оставляем как есть
		payloadBytes = dbTask.Payload
	}

	// Конвертируем result из JSONB
	var resultBytes []byte
	if dbTask.Result != nil {
		if err := json.Unmarshal(*dbTask.Result, &resultBytes); err != nil {
			resultBytes = *dbTask.Result
		}
	}

	task := &models.Task{
		TaskID:          dbTask.ID,
		TaskType:        models.TaskType(dbTask.TaskType),
		Status:          models.TaskStatus(dbTask.Status),
		Priority:        dbTask.Priority,
		Payload:         payloadBytes,
		Result:          resultBytes,
		Error:           "",
		DeadlineMs:      dbTask.DeadlineMs,
		UserID:          dbTask.UserID,
		CreatedAt:       unixMsToTime(dbTask.CreatedAt),
		ExecutionTimeMs: 0,
		Progress:        0,
	}

	if dbTask.Error != nil {
		task.Error = *dbTask.Error
	}

	if dbTask.WebhookURL != nil {
		task.WebhookURL = *dbTask.WebhookURL
	}

	if dbTask.StartedAt != nil {
		startedAt := unixMsToTime(*dbTask.StartedAt)
		task.StartedAt = &startedAt
	}

	if dbTask.CompletedAt != nil {
		completedAt := unixMsToTime(*dbTask.CompletedAt)
		task.CompletedAt = &completedAt
	}

	if dbTask.ExecutionTimeMs != nil {
		task.ExecutionTimeMs = *dbTask.ExecutionTimeMs
	}

	// Кешируем задачу в Redis
	if err := r.cacheTask(ctx, task); err != nil {
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Warn(ctx, "failed to cache task in Redis", zap.String("task_id", taskID), zap.Error(err))
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	metrics.RedisCacheMisses.Inc() // Cache miss, got from DB

	return task, nil
}

// UpdateTask обновляет задачу
func (r *repositoryImpl) UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) (*models.Task, error) {
	start := time.Now()
	operation := "UpdateTask"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	// Строим UPDATE запрос динамически
	updateBuilder := r.psql.Update("tasks").Where(sq.Eq{"id": taskID})

	if update.Status != nil {
		updateBuilder = updateBuilder.Set("status", string(*update.Status))
	}

	if update.StartedAt != nil {
		updateBuilder = updateBuilder.Set("started_at", timeToUnixMs(*update.StartedAt))
	}

	if update.CompletedAt != nil {
		updateBuilder = updateBuilder.Set("completed_at", timeToUnixMs(*update.CompletedAt))
	}

	if update.Result != nil {
		resultJSON, err := json.Marshal(json.RawMessage(update.Result))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal result: %w", err)
		}
		updateBuilder = updateBuilder.Set("result", resultJSON)
	}

	if update.Error != nil {
		updateBuilder = updateBuilder.Set("error", *update.Error)
	}

	if update.ExecutionTimeMs != nil {
		updateBuilder = updateBuilder.Set("execution_time_ms", *update.ExecutionTimeMs)
	}

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Используем RETURNING для получения обновленной задачи одним запросом
	query, args, err := updateBuilder.
		Suffix("RETURNING id, user_id, task_type, payload, result, error, status, priority, deadline_ms, created_at, started_at, completed_at, execution_time_ms, webhook_url").
		ToSql()

	if err != nil {
		log.Error(ctx, "failed to build update query", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to build update query: %w", err)
	}

	var dbTask struct {
		ID              string
		UserID          string
		TaskType        string
		Payload         json.RawMessage
		Result          *json.RawMessage
		Error           *string
		Status          string
		Priority        int32
		DeadlineMs      int64
		CreatedAt       int64
		StartedAt       *int64
		CompletedAt     *int64
		ExecutionTimeMs *int64
		WebhookURL      *string
	}

	err = r.db.QueryRow(ctx, query, args...).Scan(
		&dbTask.ID, &dbTask.UserID, &dbTask.TaskType, &dbTask.Payload, &dbTask.Result,
		&dbTask.Error, &dbTask.Status, &dbTask.Priority, &dbTask.DeadlineMs,
		&dbTask.CreatedAt, &dbTask.StartedAt, &dbTask.CompletedAt, &dbTask.ExecutionTimeMs, &dbTask.WebhookURL,
	)

	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		if err == pgx.ErrNoRows {
			log.Warn(ctx, "task not found for update", zap.String("task_id", taskID))
			return nil, fmt.Errorf("task not found: %w", err)
		}
		log.Error(ctx, "failed to update task in database", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to update task: %w", err)
	}

	// Конвертируем payload из JSONB
	var payloadBytes []byte
	if err := json.Unmarshal(dbTask.Payload, &payloadBytes); err != nil {
		payloadBytes = dbTask.Payload
	}

	// Конвертируем result из JSONB
	var resultBytes []byte
	if dbTask.Result != nil {
		if err := json.Unmarshal(*dbTask.Result, &resultBytes); err != nil {
			resultBytes = *dbTask.Result
		}
	}

	task := &models.Task{
		TaskID:          dbTask.ID,
		TaskType:        models.TaskType(dbTask.TaskType),
		Status:          models.TaskStatus(dbTask.Status),
		Priority:        dbTask.Priority,
		Payload:         payloadBytes,
		Result:          resultBytes,
		Error:           "",
		DeadlineMs:      dbTask.DeadlineMs,
		UserID:          dbTask.UserID,
		CreatedAt:       unixMsToTime(dbTask.CreatedAt),
		ExecutionTimeMs: 0,
		Progress:        0,
	}

	if dbTask.Error != nil {
		task.Error = *dbTask.Error
	}

	if dbTask.WebhookURL != nil {
		task.WebhookURL = *dbTask.WebhookURL
	}

	if dbTask.StartedAt != nil {
		startedAt := unixMsToTime(*dbTask.StartedAt)
		task.StartedAt = &startedAt
	}

	if dbTask.CompletedAt != nil {
		completedAt := unixMsToTime(*dbTask.CompletedAt)
		task.CompletedAt = &completedAt
	}

	if dbTask.ExecutionTimeMs != nil {
		task.ExecutionTimeMs = *dbTask.ExecutionTimeMs
	}

	// Кешируем обновленную задачу в Redis
	if err := r.cacheTask(ctx, task); err != nil {
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Warn(ctx, "failed to cache updated task in Redis", zap.String("task_id", taskID), zap.Error(err))
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	return task, nil
}

// CancelTask отменяет задачу по ID
func (r *repositoryImpl) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	start := time.Now()
	operation := "CancelTask"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Проверяем текущий статус задачи
	currentTask, err := r.GetTaskByID(ctx, taskID)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		return nil, fmt.Errorf("failed to get task for cancellation: %w", err)
	}

	// Проверяем, что задачу можно отменить
	if currentTask.Status == models.TaskStatusCompleted || currentTask.Status == models.TaskStatusFailed {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Warn(ctx, "cannot cancel task in terminal state",
			zap.String("task_id", taskID),
			zap.String("status", string(currentTask.Status)))
		return currentTask, fmt.Errorf("task is already in terminal state: %s", currentTask.Status)
	}

	// Обновляем статус на cancelled
	cancelledStatus := models.TaskStatusCancelled
	now := time.Now()
	update := &models.TaskUpdate{
		Status:      &cancelledStatus,
		CompletedAt: &now,
	}

	task, err := r.UpdateTask(ctx, taskID, update)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to update task to cancelled", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to cancel task: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	log.Info(ctx, "task cancelled successfully", zap.String("task_id", taskID))

	return task, nil
}

// ListTasks возвращает список задач по фильтру
func (r *repositoryImpl) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
	start := time.Now()
	operation := "ListTasks"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	queryBuilder := r.psql.
		Select("id", "user_id", "task_type", "payload", "result", "error", "status", "priority", "deadline_ms",
			"created_at", "started_at", "completed_at", "execution_time_ms", "webhook_url").
		From("tasks")

	// Применяем фильтры
	if filter.UserID != "" {
		queryBuilder = queryBuilder.Where(sq.Eq{"user_id": filter.UserID})
	}

	if filter.StatusFilter != "" && filter.StatusFilter != "all" {
		queryBuilder = queryBuilder.Where(sq.Eq{"status": filter.StatusFilter})
	} else if filter.Status != "" {
		queryBuilder = queryBuilder.Where(sq.Eq{"status": string(filter.Status)})
	}

	// Применяем лимит
	if filter.Limit > 0 {
		queryBuilder = queryBuilder.Limit(uint64(filter.Limit))
	}

	// Сортируем по created_at DESC
	queryBuilder = queryBuilder.OrderBy("created_at DESC")

	log := logger.GetOrCreateLoggerFromCtx(ctx)
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		log.Error(ctx, "failed to build select query for list tasks", zap.Error(err))
		return nil, fmt.Errorf("failed to build select query: %w", err)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to query tasks from database", zap.Error(err))
		return nil, fmt.Errorf("failed to query tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*models.Task
	for rows.Next() {
		var dbTask struct {
			ID              string
			UserID          string
			TaskType        string
			Payload         json.RawMessage
			Result          *json.RawMessage
			Error           *string
			Status          string
			Priority        int32
			DeadlineMs      int64
			CreatedAt       int64
			StartedAt       *int64
			CompletedAt     *int64
			ExecutionTimeMs *int64
			WebhookURL      *string
		}

		err := rows.Scan(
			&dbTask.ID, &dbTask.UserID, &dbTask.TaskType, &dbTask.Payload, &dbTask.Result,
			&dbTask.Error, &dbTask.Status, &dbTask.Priority, &dbTask.DeadlineMs,
			&dbTask.CreatedAt, &dbTask.StartedAt, &dbTask.CompletedAt, &dbTask.ExecutionTimeMs, &dbTask.WebhookURL,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		// Конвертируем payload из JSONB
		var payloadBytes []byte
		if err := json.Unmarshal(dbTask.Payload, &payloadBytes); err != nil {
			payloadBytes = dbTask.Payload
		}

		// Конвертируем result из JSONB
		var resultBytes []byte
		if dbTask.Result != nil {
			if err := json.Unmarshal(*dbTask.Result, &resultBytes); err != nil {
				resultBytes = *dbTask.Result
			}
		}

		task := &models.Task{
			TaskID:          dbTask.ID,
			TaskType:        models.TaskType(dbTask.TaskType),
			Status:          models.TaskStatus(dbTask.Status),
			Priority:        dbTask.Priority,
			Payload:         payloadBytes,
			Result:          resultBytes,
			Error:           "",
			DeadlineMs:      dbTask.DeadlineMs,
			UserID:          dbTask.UserID,
			CreatedAt:       unixMsToTime(dbTask.CreatedAt),
			ExecutionTimeMs: 0,
			Progress:        0,
		}

		if dbTask.Error != nil {
			task.Error = *dbTask.Error
		}

		if dbTask.WebhookURL != nil {
			task.WebhookURL = *dbTask.WebhookURL
		}

		if dbTask.StartedAt != nil {
			startedAt := unixMsToTime(*dbTask.StartedAt)
			task.StartedAt = &startedAt
		}

		if dbTask.CompletedAt != nil {
			completedAt := unixMsToTime(*dbTask.CompletedAt)
			task.CompletedAt = &completedAt
		}

		if dbTask.ExecutionTimeMs != nil {
			task.ExecutionTimeMs = *dbTask.ExecutionTimeMs
		}

		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "error iterating tasks", zap.Error(err))
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	return tasks, nil
}

// cacheTask кеширует задачу в Redis
func (r *repositoryImpl) cacheTask(ctx context.Context, task *models.Task) error {
	start := time.Now()
	operation := "cache_set"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.RedisOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	taskJson, err := json.Marshal(task)
	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Error(ctx, "failed to marshal task for Redis cache", zap.String("task_id", task.TaskID), zap.Error(err))
		return fmt.Errorf("failed to marshal task for Redis cache: %w", err)
	}
	key := r.getRedisKey(task.TaskID)
	err = r.client.Set(ctx, key, taskJson, constants.TaskCacheTTL).Err()
	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Error(ctx, "failed to cache task", zap.String("task_id", task.TaskID), zap.Error(err))
	} else {
		metrics.RedisOperationTotal.WithLabelValues(operation, "success").Inc()
	}
	return err
}

// getTaskFromCache получает задачу из Redis кеша
func (r *repositoryImpl) getTaskFromCache(ctx context.Context, taskID string) (*models.Task, error) {
	start := time.Now()
	operation := "cache_get"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.RedisOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	key := r.getRedisKey(taskID)
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			metrics.RedisOperationTotal.WithLabelValues(operation, "not_found").Inc()
			return nil, nil
		}
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Error(ctx, "failed to get task from cache", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to get task from cache: %w", err)
	}

	var task models.Task
	if err := json.Unmarshal([]byte(val), &task); err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Error(ctx, "failed to unmarshal task from cache", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal task from cache: %w", err)
	}

	metrics.RedisOperationTotal.WithLabelValues(operation, "success").Inc()
	return &task, nil
}
