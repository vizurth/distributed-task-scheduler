package app

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/worker/client"
	"github.com/vizurth/distributed-task-scheduler/internal/worker/executor"
	"go.uber.org/zap"
)

type App struct {
	workerID string
	log      *logger.Logger
	client   *client.ProcessorClient
	executor *executor.TaskExecutor
}

func New(ctx context.Context, workerID string, config *config.Config) (*App, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	processClient, err := client.NewProcessorClient(ctx, fmt.Sprintf("%s:%s", config.Processor.Host, config.Processor.Port), workerID)
	if err != nil {
		log.Error(ctx, "failed to up client", zap.Error(err))
		return nil, err
	}

	taskExecutor := executor.NewTaskExecutor()

	return &App{
		workerID: workerID,
		log:      log,
		client:   processClient,
		executor: taskExecutor,
	}, nil
}

func (a *App) Run(ctx context.Context) {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		a.log.Info(ctx, fmt.Sprintf("start worker"))
		if err := a.client.ProcessTask(ctx, a.executor); err != nil {
			log.Fatal(ctx, "failed to start worker", zap.Error(err))
		}
	}()
	<-ctx.Done()
	a.log.Info(ctx, "shutting down gRPC server...")

	a.Shutdown(ctx)
}

func (a *App) Shutdown(ctx context.Context) {
	a.client.Close()
	a.log.Info(ctx, "worker shutdown complete")
}
