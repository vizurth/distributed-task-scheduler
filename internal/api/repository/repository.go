package repository

import (
	"context"
	"encoding/json"
	"errors"
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

	taskSelectColumns = "id, user_id, task_type, payload, result, error, status, priority, deadline_ms, " +
		"created_at, started_at, completed_at, execution_time_ms, webhook_url"
)

// dbTaskRow — внутренняя структура для сканирования строки задачи из БД.
type dbTaskRow struct {
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

func (r *repositoryImpl) getRedisKey(taskID string) string {
	return redisTaskKeyPrefix + taskID
}

// unixMsToTime конвертирует Unix timestamp в миллисекундах в time.Time.
func unixMsToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

// scanDbTask сканирует строку из pgx в dbTaskRow.
func scanDbTask(scanner interface {
	Scan(dest ...any) error
}) (dbTaskRow, error) {
	var row dbTaskRow
	err := scanner.Scan(
		&row.ID, &row.UserID, &row.TaskType, &row.Payload, &row.Result,
		&row.Error, &row.Status, &row.Priority, &row.DeadlineMs,
		&row.CreatedAt, &row.StartedAt, &row.CompletedAt, &row.ExecutionTimeMs, &row.WebhookURL,
	)
	return row, err
}

// dbRowToTask конвертирует dbTaskRow в models.Task.
func dbRowToTask(row dbTaskRow) *models.Task {
	var payloadBytes []byte
	if err := json.Unmarshal(row.Payload, &payloadBytes); err != nil {
		payloadBytes = row.Payload
	}

	var resultBytes []byte
	if row.Result != nil {
		if err := json.Unmarshal(*row.Result, &resultBytes); err != nil {
			resultBytes = *row.Result
		}
	}

	task := &models.Task{
		TaskID:     row.ID,
		TaskType:   models.TaskType(row.TaskType),
		Status:     models.TaskStatus(row.Status),
		Priority:   row.Priority,
		Payload:    payloadBytes,
		Result:     resultBytes,
		DeadlineMs: row.DeadlineMs,
		UserID:     row.UserID,
		CreatedAt:  unixMsToTime(row.CreatedAt),
	}

	if row.Error != nil {
		task.Error = *row.Error
	}
	if row.WebhookURL != nil {
		task.WebhookURL = *row.WebhookURL
	}
	if row.StartedAt != nil {
		t := unixMsToTime(*row.StartedAt)
		task.StartedAt = &t
	}
	if row.CompletedAt != nil {
		t := unixMsToTime(*row.CompletedAt)
		task.CompletedAt = &t
	}
	if row.ExecutionTimeMs != nil {
		task.ExecutionTimeMs = *row.ExecutionTimeMs
	}

	return task
}

// CreateTask создаёт новую задачу в БД.
func (r *repositoryImpl) CreateTask(ctx context.Context, task *models.TaskCreate) (*models.Task, error) {
	start := time.Now()
	operation := "CreateTask"

	defer func() {
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	taskID := uuid.New().String()
	now := time.Now()

	payloadJSON, err := json.Marshal(json.RawMessage(task.Payload))
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to marshal payload", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	query, args, err := r.psql.
		Insert("tasks").
		Columns("id", "user_id", "task_type", "payload", "status", "priority", "deadline_ms", "created_at", "webhook_url").
		Values(taskID, task.UserID, string(task.TaskType), payloadJSON, string(models.TaskStatusPending),
			task.Priority, task.DeadlineMs, now.UnixMilli(), task.WebhookURL).
		ToSql()
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to build insert query", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to build insert query: %w", err)
	}

	if _, err = r.db.Exec(ctx, query, args...); err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to insert task into database", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to insert task: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()

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
	}

	if err := r.cacheTask(ctx, createdTask); err != nil {
		log.Warn(ctx, "failed to cache task in Redis", zap.String("task_id", taskID), zap.Error(err))
	}

	return createdTask, nil
}

// GetTaskByID возвращает задачу по ID.
func (r *repositoryImpl) GetTaskByID(ctx context.Context, taskID string) (*models.Task, error) {
	start := time.Now()
	operation := "GetTaskByID"

	defer func() {
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	// Проверяем Redis-кеш
	if cachedTask, err := r.getTaskFromCache(ctx, taskID); err == nil && cachedTask != nil {
		metrics.RedisCacheHits.Inc()
		metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
		return cachedTask, nil
	}

	metrics.RedisCacheMisses.Inc()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	query, args, err := r.psql.
		Select(taskSelectColumns).
		From("tasks").
		Where(sq.Eq{"id": taskID}).
		ToSql()
	if err != nil {
		log.Error(ctx, "failed to build select query", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to build select query: %w", err)
	}

	row, err := scanDbTask(r.db.QueryRow(ctx, query, args...))
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		if errors.Is(err, pgx.ErrNoRows) {
			log.Warn(ctx, "task not found in database", zap.String("task_id", taskID))
			return nil, fmt.Errorf("%s: %w", constants.ErrMsgTaskNotFound, err)
		}
		log.Error(ctx, "failed to query task from database", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to query task: %w", err)
	}

	task := dbRowToTask(row)

	if err := r.cacheTask(ctx, task); err != nil {
		log.Warn(ctx, "failed to cache task in Redis", zap.String("task_id", taskID), zap.Error(err))
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	return task, nil
}

// UpdateTask обновляет задачу через RETURNING.
func (r *repositoryImpl) UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) (*models.Task, error) {
	start := time.Now()
	operation := "UpdateTask"

	defer func() {
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	updateBuilder := r.psql.Update("tasks").Where(sq.Eq{"id": taskID})

	if update.Status != nil {
		updateBuilder = updateBuilder.Set("status", string(*update.Status))
	}
	if update.StartedAt != nil {
		updateBuilder = updateBuilder.Set("started_at", update.StartedAt.UnixMilli())
	}
	if update.CompletedAt != nil {
		updateBuilder = updateBuilder.Set("completed_at", update.CompletedAt.UnixMilli())
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

	query, args, err := updateBuilder.
		Suffix("RETURNING " + taskSelectColumns).
		ToSql()
	if err != nil {
		log.Error(ctx, "failed to build update query", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to build update query: %w", err)
	}

	row, err := scanDbTask(r.db.QueryRow(ctx, query, args...))
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		if errors.Is(err, pgx.ErrNoRows) {
			log.Warn(ctx, "task not found for update", zap.String("task_id", taskID))
			return nil, fmt.Errorf("%s: %w", constants.ErrMsgTaskNotFound, err)
		}
		log.Error(ctx, "failed to update task in database", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to update task: %w", err)
	}

	task := dbRowToTask(row)

	if err := r.cacheTask(ctx, task); err != nil {
		log.Warn(ctx, "failed to cache updated task in Redis", zap.String("task_id", taskID), zap.Error(err))
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	return task, nil
}

// CancelTask отменяет задачу по ID.
func (r *repositoryImpl) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	start := time.Now()
	operation := "CancelTask"

	defer func() {
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	currentTask, err := r.GetTaskByID(ctx, taskID)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		return nil, fmt.Errorf("failed to get task for cancellation: %w", err)
	}

	if currentTask.Status == models.TaskStatusCompleted || currentTask.Status == models.TaskStatusFailed {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Warn(ctx, "cannot cancel task in terminal state",
			zap.String("task_id", taskID),
			zap.String("status", string(currentTask.Status)))
		return currentTask, fmt.Errorf("%s: %s", constants.ErrMsgTaskAlreadyInTerminalState, currentTask.Status)
	}

	cancelledStatus := models.TaskStatusCancelled
	now := time.Now()
	task, err := r.UpdateTask(ctx, taskID, &models.TaskUpdate{
		Status:      &cancelledStatus,
		CompletedAt: &now,
	})
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to update task to cancelled", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to cancel task: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	log.Info(ctx, "task cancelled successfully", zap.String("task_id", taskID))
	return task, nil
}

// ListTasks возвращает список задач по фильтру.
func (r *repositoryImpl) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
	start := time.Now()
	operation := "ListTasks"

	defer func() {
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	queryBuilder := r.psql.
		Select(taskSelectColumns).
		From("tasks").
		OrderBy("created_at DESC")

	if filter.UserID != "" {
		queryBuilder = queryBuilder.Where(sq.Eq{"user_id": filter.UserID})
	}
	if filter.StatusFilter != "" && filter.StatusFilter != "all" {
		queryBuilder = queryBuilder.Where(sq.Eq{"status": filter.StatusFilter})
	} else if filter.Status != "" {
		queryBuilder = queryBuilder.Where(sq.Eq{"status": string(filter.Status)})
	}
	if filter.Limit > 0 {
		queryBuilder = queryBuilder.Limit(uint64(filter.Limit))
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		log.Error(ctx, "failed to build list tasks query", zap.Error(err))
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
		row, err := scanDbTask(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}
		tasks = append(tasks, dbRowToTask(row))
	}

	if err := rows.Err(); err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "error iterating tasks", zap.Error(err))
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()
	return tasks, nil
}

// cacheTask сохраняет задачу в Redis.
func (r *repositoryImpl) cacheTask(ctx context.Context, task *models.Task) error {
	start := time.Now()
	operation := "cache_set"

	defer func() {
		metrics.RedisOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	data, err := json.Marshal(task)
	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to marshal task for Redis cache", zap.String("task_id", task.TaskID), zap.Error(err))
		return fmt.Errorf("failed to marshal task for Redis cache: %w", err)
	}

	if err = r.client.Set(ctx, r.getRedisKey(task.TaskID), data, constants.TaskCacheTTL).Err(); err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to set task in Redis", zap.String("task_id", task.TaskID), zap.Error(err))
		return err
	}

	metrics.RedisOperationTotal.WithLabelValues(operation, "success").Inc()
	return nil
}

// getTaskFromCache получает задачу из Redis-кеша.
func (r *repositoryImpl) getTaskFromCache(ctx context.Context, taskID string) (*models.Task, error) {
	start := time.Now()
	operation := "cache_get"

	defer func() {
		metrics.RedisOperationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	val, err := r.client.Get(ctx, r.getRedisKey(taskID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			metrics.RedisOperationTotal.WithLabelValues(operation, "not_found").Inc()
			return nil, nil
		}
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to get task from cache", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to get task from cache: %w", err)
	}

	var task models.Task
	if err := json.Unmarshal([]byte(val), &task); err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to unmarshal task from cache", zap.String("task_id", taskID), zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal task from cache: %w", err)
	}

	metrics.RedisOperationTotal.WithLabelValues(operation, "success").Inc()
	return &task, nil
}
