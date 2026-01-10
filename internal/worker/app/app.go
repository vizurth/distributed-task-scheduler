package app

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
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

	taskExecutor := executor.NewTaskExecutor(workerID)

	// Set initial worker status as healthy
	metrics.WorkerStatus.WithLabelValues(workerID).Set(1)
	metrics.WorkerProcessingTasks.WithLabelValues(workerID).Set(0)

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

	go a.monitorHealth(ctx)

	<-ctx.Done()
	a.log.Info(ctx, "shutting down worker...")
	a.Shutdown(ctx)
}

func (a *App) Shutdown(ctx context.Context) {
	metrics.WorkerStatus.WithLabelValues(a.workerID).Set(0)
	a.client.Close()
	a.log.Info(ctx, "worker shutdown complete")
}

func (a *App) monitorHealth(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.WorkerStatus.WithLabelValues(a.workerID).Set(1)
		}
	}
}
