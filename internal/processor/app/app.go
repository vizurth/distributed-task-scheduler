package app

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"github.com/vizurth/distributed-task-scheduler/internal/postgres"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/handler"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/manager"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/repository"
	"github.com/vizurth/distributed-task-scheduler/internal/processor/service"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	myredis "github.com/vizurth/distributed-task-scheduler/internal/redis"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type App struct {
	config        *config.Config
	log           *logger.Logger
	pool          *pgxpool.Pool
	client        *redis.Client
	server        *grpc.Server
	workerManager *manager.WorkerManager
	consumer      queue.Consumer
	producer      queue.Producer
	taskQueue     chan *models.KafkaTaskMessage
}

func New(ctx context.Context, config *config.Config) (*App, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)

	pool, err := postgres.New(ctx, config.Postgres)
	if err != nil {
		log.Error(ctx, "failed to connect postgres", zap.Error(err))
		return nil, err
	}

	client, err := myredis.NewClient(ctx, config.Redis)
	if err != nil {
		log.Error(ctx, "failed to connect redis", zap.Error(err))
	}

	consumer, err := queue.NewConsumer(&config.Kafka, "tasks-new")
	if err != nil {
		log.Error(ctx, "failed create consumer", zap.Error(err))
		return nil, err
	}

	producer, err := queue.NewProducer(&config.Kafka)
	if err != nil {
		log.Error(ctx, "failed create producer", zap.Error(err))
		return nil, err
	}
	taskQueue := make(chan *models.KafkaTaskMessage, 1024)
	workerManager := manager.NewWorkerManager()

	processorRepo := repository.NewRepository(pool, client)
	processorService := service.NewService(processorRepo, producer)
	processorHandler := handler.NewHandler(processorService, pool, client, producer, workerManager, taskQueue)
	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(100000),
	)

	processpb.RegisterTaskProcessorServer(grpcServer, processorHandler)

	return &App{
		config:        config,
		log:           log,
		pool:          pool,
		client:        client,
		server:        grpcServer,
		workerManager: workerManager,
		consumer:      consumer,
		producer:      producer,
		taskQueue:     taskQueue,
	}, nil

}

func (a *App) Run(ctx context.Context) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", a.config.Processor.Port))
	if err != nil {
		a.log.Fatal(ctx, "failed to listen for GRPC", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go a.distributedTasksFromKafka(ctx)

	go func() {
		a.log.Info(ctx, fmt.Sprintf("GRPC server listening on port %s", a.config.Processor.Port))
		if err = a.server.Serve(lis); err != nil {
			log.Fatal(ctx, "gRPC server failed", zap.Error(err))
		}
	}()
	<-ctx.Done()
	a.log.Info(ctx, "shutting down gRPC server...")

	a.Shutdown(ctx)

}

func (a *App) Shutdown(ctx context.Context) {
	a.server.GracefulStop()
	a.pool.Close()
	a.client.Close()
	a.consumer.Close()
	a.producer.Close()
	a.log.Info(ctx, "processor shutdown complete")
}

func (a *App) distributedTasksFromKafka(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := a.consumer.Read(ctx)
			if err != nil {
				a.log.Error(ctx, "failed to read task from kafka", zap.Error(err))
				continue
			}
			if msg.Type != "task" {
				continue
			}
			kafkaTask := msg.Data.(*models.KafkaTaskMessage)

			a.log.Info(ctx, "received task from kafka",
				zap.String("task_id", kafkaTask.TaskID),
				zap.String("task_type", kafkaTask.TaskType),
				zap.Int32("priority", kafkaTask.Priority),
			)

			a.taskQueue <- kafkaTask

			// TODO: Добавить метрику
		}

	}
}
