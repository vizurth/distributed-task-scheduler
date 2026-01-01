package main

import (
	"context"
	"log"

	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/app"
)

func main() {
	// Application entry point
	if err := metrics.RegisterMetrics(); err != nil {
		panic("failed to register metrics" + err.Error())
	}

	metricsPort := "8002"
	metrics.StartMetricsServer(metricsPort)

	ctx := context.Background()
	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	processor, err := app.New(ctx, cfg)
	if err != nil {
		log.Fatalf("Error starting processor: %v", err)
	}

	processor.Run(ctx)
}
