package main

import (
	"context"

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

	metricsPort := "8001"
	metrics.StartMetricsServer(metricsPort, func(err error) {
		panic("metrics server failed: " + err.Error())
	})

	ctx := context.Background()
	cfg, err := config.New()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	if err != nil {
		log.Fatal(ctx, "failed to load config", zap.Error(err))
	}

	application, err := app.New(ctx, cfg)
	if err != nil {
		log.Fatal(ctx, "failed to initialize application", zap.Error(err))
	}

	application.Run(ctx)
}
