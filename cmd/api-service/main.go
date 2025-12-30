package main

import (
	"context"
	"os"

	"github.com/vizurth/distributed-task-scheduler/internal/api/app"
	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"go.uber.org/zap"
)

func main() {
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
