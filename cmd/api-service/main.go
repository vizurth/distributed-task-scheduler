package main

import (
	"context"
	"os"

	"github.com/vizurth/distributed-task-scheduler/internal/api/app"
	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"go.uber.org/zap"
)

func main() {
	if err := metrics.RegisterMetrics(); err != nil {
		panic("failed to register metrics: " + err.Error())
	}

	// Запусти metrics HTTP server на порту 8001 с health checks
	metricsPort := "8001"
	metrics.StartMetricsServer(metricsPort)

	ctx := context.Background()
	cfg, err := config.New()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	if err != nil {
		log.Fatal(ctx, "failed to lad config")
	}

	application, err := app.New(ctx, cfg)
	if err != nil {
		log.Fatal(ctx, "failed to initialize application", zap.Error(err))
		os.Exit(1)
	}

	application.Run(ctx)
}
