// cmd/worker/main.go
package main

import (
	"context"
	"fmt"

	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/worker/app"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	ctx, _, err := logger.New(ctx)
	if err != nil {
		panic(err)
	}

	log := logger.GetOrCreateLoggerFromCtx(ctx)
	cfg, err := config.New()

	// Генерируй уникальный ID для worker'а
	workerID := fmt.Sprint("worker-1") // TODO: сделать ID из конфига

	worker, err := app.New(ctx, workerID, cfg)
	if err != nil {
		log.Fatal(ctx, "failed to initialize worker", zap.Error(err))
	}

	worker.Run(ctx)
}
