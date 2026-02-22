package main

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/app"
	"go.uber.org/zap"
)

func main() {
	if err := metrics.RegisterMetrics(); err != nil {
		panic("failed to register metrics: " + err.Error())
	}

	metricsPort := "8002"
	metrics.StartMetricsServer(metricsPort, func(err error) {
		panic("metrics server failed: " + err.Error())
	})

	ctx := context.Background()
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	cfg, err := config.New()
	if err != nil {
		log.Fatal(ctx, "failed to load config", zap.Error(err))
	}

	processor, err := app.New(ctx, cfg)
	if err != nil {
		log.Fatal(ctx, "failed to initialize processor", zap.Error(err))
	}

	processor.Run(ctx)
}
