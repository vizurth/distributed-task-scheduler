package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
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

func (r *repositoryImpl) getRedisKey(taskID string) string {
	return redisTaskKeyPrefix + taskID
}

// UpdateTask обновляет задачу в БД
func (r *repositoryImpl) UpdateTask(ctx context.Context, taskID string, update *models.TaskUpdate) error {
	start := time.Now()
	operation := "UpdateTask"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Строим UPDATE запрос динамически
	updateBuilder := r.psql.Update("tasks").Where(sq.Eq{"id": taskID})

	if update.Status != nil {
		updateBuilder = updateBuilder.Set("status", string(*update.Status))
	}

	if update.Result != nil {
		resultJSON, err := json.Marshal(json.RawMessage(update.Result))
		if err != nil {
			metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
			return fmt.Errorf("failed to marshal result: %w", err)
		}
		updateBuilder = updateBuilder.Set("result", resultJSON)
	}

	if update.CompletedAt != nil {
		updateBuilder = updateBuilder.Set("completed_at", update.CompletedAt.UnixMilli())
	}

	if update.ExecutionTimeMs != nil {
		updateBuilder = updateBuilder.Set("execution_time_ms", *update.ExecutionTimeMs)
	}

	if update.Error != nil {
		updateBuilder = updateBuilder.Set("error", *update.Error)
	}

	query, args, err := updateBuilder.ToSql()
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to build update query", zap.String("task_id", taskID), zap.Error(err))
		return fmt.Errorf("cannot update task: %w", err)
	}

	_, err = r.db.Exec(ctx, query, args...)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to execute update query", zap.String("task_id", taskID), zap.Error(err))
		return fmt.Errorf("update task failed: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()

	// Асинхронно обновляем кеш
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.updateTaskCacheFields(ctx, taskID, update)
	}()

	return nil
}

// UpdateTaskStatus обновляет статус задачи и время начала выполнения
func (r *repositoryImpl) UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, workerID string, currTime time.Time) error {
	start := time.Now()
	operation := "UpdateTaskStatus"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.DBOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	query, args, err := r.psql.Update("tasks").
		Set("status", string(status)).
		Set("started_at", currTime.UnixMilli()).
		Where(sq.Eq{"id": taskID}).
		ToSql()

	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to build update status query", zap.String("task_id", taskID), zap.Error(err))
		return fmt.Errorf("cannot update task status: %w", err)
	}

	_, err = r.db.Exec(ctx, query, args...)
	if err != nil {
		metrics.DBOperationTotal.WithLabelValues(operation, "error").Inc()
		log.Error(ctx, "failed to execute update status query", zap.String("task_id", taskID), zap.Error(err))
		return fmt.Errorf("update task status failed: %w", err)
	}

	metrics.DBOperationTotal.WithLabelValues(operation, "success").Inc()

	// Асинхронно обновляем кеш
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.updateTaskStatusCacheFields(ctx, taskID, status, workerID, currTime)
	}()

	return nil
}

// updateTaskCacheFields обновляет поля задачи в Redis кеше
func (r *repositoryImpl) updateTaskCacheFields(ctx context.Context, taskID string, update *models.TaskUpdate) error {
	cachedTask, err := r.GetTaskFromCache(ctx, taskID)
	if err != nil {
		return err
	}

	if cachedTask == nil {
		return nil
	}

	if update.Status != nil {
		cachedTask.Status = *update.Status
	}
	if update.Result != nil {
		cachedTask.Result = update.Result
	}
	if update.CompletedAt != nil {
		cachedTask.CompletedAt = update.CompletedAt
	}
	if update.ExecutionTimeMs != nil {
		cachedTask.ExecutionTimeMs = *update.ExecutionTimeMs
	}
	if update.Error != nil {
		cachedTask.Error = *update.Error
	}
	if update.Progress != nil {
		cachedTask.Progress = *update.Progress
	}

	return r.CacheTask(ctx, cachedTask)
}

// updateTaskStatusCacheFields обновляет статус задачи в Redis кеше
func (r *repositoryImpl) updateTaskStatusCacheFields(ctx context.Context, taskID string, status models.TaskStatus, workerID string, currTime time.Time) error {
	cachedTask, err := r.GetTaskFromCache(ctx, taskID)
	if err != nil {
		return err
	}

	if cachedTask == nil {
		return nil
	}

	cachedTask.Status = status
	cachedTask.StartedAt = &currTime

	return r.CacheTask(ctx, cachedTask)
}

// CacheTask кеширует задачу в Redis
func (r *repositoryImpl) CacheTask(ctx context.Context, task *models.Task) error {
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

// GetTaskFromCache получает задачу из Redis кеша
func (r *repositoryImpl) GetTaskFromCache(ctx context.Context, taskID string) (*models.Task, error) {
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

// InvalidateTaskCache удаляет задачу из Redis кеша
func (r *repositoryImpl) InvalidateTaskCache(ctx context.Context, taskID string) error {
	start := time.Now()
	operation := "cache_delete"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.RedisOperationDuration.WithLabelValues(operation).Observe(duration)
	}()

	key := r.getRedisKey(taskID)
	err := r.client.Del(ctx, key).Err()
	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues(operation, "error").Inc()
		log := logger.GetOrCreateLoggerFromCtx(ctx)
		log.Error(ctx, "failed to invalidate task cache", zap.String("task_id", taskID), zap.Error(err))
		return fmt.Errorf("failed to invalidate task cache: %w", err)
	}
	metrics.RedisOperationTotal.WithLabelValues(operation, "success").Inc()
	return nil
}
