package app

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/vizurth/distributed-task-scheduler/internal/api/handler"
	"github.com/vizurth/distributed-task-scheduler/internal/api/repository"
	"github.com/vizurth/distributed-task-scheduler/internal/api/service"
	"github.com/vizurth/distributed-task-scheduler/internal/config"
	"github.com/vizurth/distributed-task-scheduler/internal/grpc/interceptors"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/postgres"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
	myredis "github.com/vizurth/distributed-task-scheduler/internal/redis"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type App struct {
	config   *config.Config
	log      *logger.Logger
	pool     *pgxpool.Pool
	redis    *redis.Client
	producer queue.Producer
	server   *grpc.Server
}

func New(ctx context.Context, config *config.Config) (*App, error) {
	ctx, _, err := logger.New(ctx)
	if err != nil {
		return nil, err
	}

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	pool, err := postgres.New(ctx, config.Postgres)
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			stats := pool.Stat()
			metrics.DBConnectionsAvailable.Set(float64(stats.TotalConns()))
			metrics.DBConnectionsInUse.Set(float64(stats.AcquiredConns()))
		}
	}()

	err = postgres.Migrate(ctx, config.Postgres)
	if err != nil {
		return nil, err
	}

	redisClient, err := myredis.NewClient(ctx, config.Redis)
	if err != nil {
		return nil, err
	}

	producer, err := queue.NewProducer(&config.Kafka)
	if err != nil {
		return nil, err
	}

	taskRepo := repository.NewRepository(pool, redisClient)
	taskService := service.NewService(taskRepo, producer)
	taskHandler := handler.NewHandler(taskService)

	opts := []grpc.ServerOption{
		// Увеличиваем лимиты
		grpc.MaxConcurrentStreams(100000),
		grpc.MaxHeaderListSize(32 * 1024),
		grpc.InitialConnWindowSize(32 * 1024 * 1024),
		grpc.InitialWindowSize(16 * 1024 * 1024),

		// Оптимальный keepalive
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    15 * time.Second,
			Timeout: 5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),

		// Увеличиваем буферы
		grpc.WriteBufferSize(64 * 1024),
		grpc.ReadBufferSize(64 * 1024),

		grpc.UnaryInterceptor(
			interceptors.TimeoutInterceptor(4 * time.Second),
		),
	}

	grpcServer := grpc.NewServer(opts...)

	taskpb.RegisterTaskAPIServer(grpcServer, taskHandler)

	return &App{
		config:   config,
		log:      log,
		pool:     pool,
		redis:    redisClient,
		producer: producer,
		server:   grpcServer,
	}, nil
}

func (a *App) Run(ctx context.Context) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", a.config.Api.Port))
	if err != nil {
		a.log.Fatal(ctx, "failed to listen for GRPC", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		a.log.Info(ctx, fmt.Sprintf("GRPC server listening on port %s", a.config.Api.Port))
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

	if err := a.redis.Close(); err != nil {
		a.log.Error(ctx, "failed to close redis connection", zap.Error(err))
	}
	a.redis.Close()
	a.producer.Close()

	a.log.Info(ctx, "successfully shutdown gRPC server")
}
