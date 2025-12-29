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
	"github.com/vizurth/distributed-task-scheduler/internal/models"
)

const (
	redisTaskKeyPrefix = "task:"
	redisTaskTTL       = time.Hour
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
	taskID := uuid.New().String()
	now := time.Now()
	createdAtMs := timeToUnixMs(now)

	// Конвертируем payload в JSONB
	payloadJSON, err := json.Marshal(json.RawMessage(task.Payload))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Строим SQL запрос с помощью squirrel
	query, args, err := r.psql.
		Insert("tasks").
		Columns("id", "user_id", "task_type", "payload", "status", "priority", "deadline_ms", "created_at", "webhook_url").
		Values(taskID, task.UserID, string(task.TaskType), payloadJSON, string(models.TaskStatusPending), task.Priority, task.DeadlineMs, createdAtMs, task.WebhookURL).
		ToSql()

	if err != nil {
		return nil, fmt.Errorf("failed to build insert query: %w", err)
	}

	_, err = r.db.Exec(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert task: %w", err)
	}

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
		// Логируем ошибку, но не прерываем выполнение
		_ = err
	}

	return createdTask, nil
}

// GetTaskByID возвращает задачу по ID
func (r *repositoryImpl) GetTaskByID(ctx context.Context, taskID string) (*models.Task, error) {
	// Сначала проверяем Redis кеш
	cachedTask, err := r.getTaskFromCache(ctx, taskID)
	if err == nil && cachedTask != nil {
		return cachedTask, nil
	}

	// Если в кеше нет, получаем из PostgreSQL
	query, args, err := r.psql.
		Select("id", "user_id", "task_type", "payload", "result", "error", "status", "priority", "deadline_ms",
			"created_at", "started_at", "completed_at", "execution_time_ms", "webhook_url").
		From("tasks").
		Where(sq.Eq{"id": taskID}).
		ToSql()

	if err != nil {
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
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("task not found: %w", err)
		}
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
		// Логируем ошибку, но не прерываем выполнение
		_ = err
	}

	return task, nil
}

// UpdateTask обновляет задачу
func (r *repositoryImpl) UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) (*models.Task, error) {
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

	// Используем RETURNING для получения обновленной задачи одним запросом
	query, args, err := updateBuilder.
		Suffix("RETURNING id, user_id, task_type, payload, result, error, status, priority, deadline_ms, created_at, started_at, completed_at, execution_time_ms, webhook_url").
		ToSql()

	if err != nil {
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
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("task not found: %w", err)
		}
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
		// Логируем ошибку, но не прерываем выполнение
		_ = err
	}

	return task, nil
}

// ListTasks возвращает список задач по фильтру
func (r *repositoryImpl) ListTasks(ctx context.Context, filter *models.TaskFilter) ([]*models.Task, error) {
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

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build select query: %w", err)
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
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
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}

	return tasks, nil
}

// CancelTask обновляет статус задачи на cancelled
func (r *repositoryImpl) CancelTask(ctx context.Context, taskID string) (*models.Task, error) {
	update := &models.TaskUpdate{
		Status: func() *models.TaskStatus {
			status := models.TaskStatusCancelled
			return &status
		}(),
	}

	return r.UpdateTask(ctx, taskID, update)
}

// cacheTask кеширует задачу в Redis
func (r *repositoryImpl) cacheTask(ctx context.Context, task *models.Task) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	key := r.getRedisKey(task.TaskID)
	return r.client.Set(ctx, key, taskJSON, redisTaskTTL).Err()
}

// getTaskFromCache получает задачу из Redis кеша
func (r *repositoryImpl) getTaskFromCache(ctx context.Context, taskID string) (*models.Task, error) {
	key := r.getRedisKey(taskID)
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var task models.Task
	if err := json.Unmarshal([]byte(val), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}
