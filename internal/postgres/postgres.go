package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"go.uber.org/zap"
)

// Config содержит настройки для подключения к Postgres
type Config struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	MaxConns int32  `yaml:"max_conns" env:"MAX_CONNS" env-default:"10"`
	MinConns int32  `yaml:"min_conns" env:"MIN_CONNS" env-default:"5"`
}

// New создает новое подключение к Postgres
func New(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	// 1. Парсим строку подключения
	poolCfg, err := pgxpool.ParseConfig(cfg.GetConnString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	// 2. Применяем настройки пула
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = 5 * time.Minute
	poolCfg.MaxConnIdleTime = 1 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second

	// (опционально) логирование
	// poolCfg.ConnConfig.Logger = pgxlog.NewLogger(...)
	// poolCfg.ConnConfig.LogLevel = pgx.LogLevelDebug

	// 3. Создаем пул
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return pool, nil
}

// Migrate выполняет миграции базы данных Postgres
func Migrate(ctx context.Context, cfg Config) error {
	// создаем строку подключения
	connString := cfg.GetConnString()
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Ищем директорию с миграциями в нескольких местах
	migrationPaths := []string{
		"migrations",
		"./migrations",
		"../migrations",
		"../../migrations",
	}

	var migrationPath string
	for _, path := range migrationPaths {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			migrationPath, _ = filepath.Abs(path)
			break
		}
	}

	if migrationPath == "" {
		return fmt.Errorf("migrations directory not found")
	}

	// создаем миграции с найденным путем
	m, err := migrate.New("file://"+migrationPath, connString)

	if err != nil {
		return fmt.Errorf("failed to create migration instance: %w", err)
	}

	// пытаемся выполнить миграции с ретраями
	retries := 5
	for i := 0; i < retries; i++ {
		err = m.Up()
		if err == nil {
			break
		}
		log.Info(ctx, "migration failed, retrying...", zap.Error(err))
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	logger.GetOrCreateLoggerFromCtx(ctx).Info(ctx, "migrated successfully")
	return nil
}

// GetConnString формирует строку подключения к Postgres
func (c *Config) GetConnString() string {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
	)
	return connString
}
