// cmd/worker/main.go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
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
	if err != nil {
		log.Fatal(ctx, "failed to load config", zap.Error(err))
	}

	if err := metrics.RegisterMetrics(); err != nil {
		log.Fatal(ctx, "failed to register metrics", zap.Error(err))
	}

	// Generate dynamic worker ID based on hostname and process ID
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("worker-%s-%d", hostname, pid)

	// Optional: Get worker instance from environment variable
	if instanceNum := os.Getenv("WORKER_INSTANCE"); instanceNum != "" {
		workerID = fmt.Sprintf("worker-%s", instanceNum)
	}

	worker, err := app.New(ctx, workerID, cfg)
	if err != nil {
		log.Fatal(ctx, "failed to initialize worker", zap.Error(err))
	}

	log.Info(ctx, "starting worker", zap.String("worker_id", workerID))
	worker.Run(ctx)
}
