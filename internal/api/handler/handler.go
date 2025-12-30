package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/api/converters"
	"github.com/vizurth/distributed-task-scheduler/internal/api/service"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type handlerImpl struct {
	taskpb.UnimplementedTaskAPIServer
	service service.Service
}

func NewHandler(service service.Service) Handler {
	return &handlerImpl{
		service: service,
	}
}

// getUserIDFromContext извлекает user_id из метаданных gRPC запроса
func getUserIDFromContext(ctx context.Context) (string, error) {
	log := logger.GetOrCreateLoggerFromCtx(ctx)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Error(ctx, "metadata not found in gRPC request")
		return "", status.Error(codes.Unauthenticated, "metadata not found")
	}

	userIDs := md.Get("user_id")
	if len(userIDs) == 0 || userIDs[0] == "" {
		log.Error(ctx, "user_id not found in metadata")
		return "", status.Error(codes.Unauthenticated, "user_id not found in metadata")
	}

	return userIDs[0], nil
}

func (h *handlerImpl) SubmitTask(ctx context.Context, req *taskpb.SubmitTaskRequest) (*taskpb.SubmitTaskResponse, error) {
	start := time.Now()
	method := "SubmitTask"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.GRPCRequestDuration.WithLabelValues(method).Observe(duration)
		metrics.TasksProcessingDuration.WithLabelValues(req.TaskType).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Извлекаем user_id из контекста
	userID, err := getUserIDFromContext(ctx)
	if err != nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "unauthorized").Inc()
		return nil, err
	}

	// Конвертируем proto запрос в внутреннюю модель
	taskCreate := converters.ProtoToTaskCreate(req, userID)

	// Создаем задачу через service
	task, err := h.service.SubmitTask(ctx, taskCreate)
	if err != nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "internal_error").Inc()
		log.Error(ctx, "failed to submit task", zap.String("user_id", userID), zap.String("task_type", req.TaskType), zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to submit task: %v", err))
	}

	metrics.GRPCRequestsTotal.WithLabelValues(method, "success").Inc()
	metrics.TasksSubmittedTotal.WithLabelValues(req.TaskType).Inc()

	// Конвертируем результат в proto ответ
	return converters.TaskToProtoSubmitResponse(task), nil
}

func (h *handlerImpl) GetTaskStatus(ctx context.Context, req *taskpb.GetTaskStatusRequest) (*taskpb.GetTaskStatusResponse, error) {
	start := time.Now()
	method := "GetTaskStatus"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.GRPCRequestDuration.WithLabelValues(method).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Получаем статус задачи через service
	task, err := h.service.GetTaskStatus(ctx, req.TaskId)
	if err != nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "internal_error").Inc()
		log.Error(ctx, "failed to get task status", zap.String("task_id", req.TaskId), zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get task status: %v", err))
	}

	if task == nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "not_found").Inc()
		log.Error(ctx, "task not found", zap.String("task_id", req.TaskId))
		return nil, status.Error(codes.NotFound, "task not found")
	}

	metrics.GRPCRequestsTotal.WithLabelValues(method, "success").Inc()

	// Конвертируем результат в proto ответ
	return converters.TaskToProtoStatusResponse(task), nil
}

func (h *handlerImpl) CancelTask(ctx context.Context, req *taskpb.CancelTaskRequest) (*taskpb.CancelTaskResponse, error) {
	start := time.Now()
	method := "CancelTask"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.GRPCRequestDuration.WithLabelValues(method).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Отменяем задачу через service
	task, err := h.service.CancelTask(ctx, req.TaskId)
	if err != nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "internal_error").Inc()
		log.Error(ctx, "failed to cancel task", zap.String("task_id", req.TaskId), zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to cancel task: %v", err))
	}

	if task == nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "not_found").Inc()
		log.Error(ctx, "task not found for cancellation", zap.String("task_id", req.TaskId))
		return nil, status.Error(codes.NotFound, "task not found")
	}

	// Проверяем, что задача действительно отменена
	success := task.Status == models.TaskStatusCancelled
	if success {
		metrics.TasksByStatus.WithLabelValues("cancelled").Inc()
	}

	metrics.GRPCRequestsTotal.WithLabelValues(method, "success").Inc()

	// Конвертируем результат в proto ответ
	return converters.TaskToProtoCancelResponse(task, success), nil
}

func (h *handlerImpl) ListTasks(req *taskpb.ListTasksRequest, stream grpc.ServerStreamingServer[taskpb.TaskInfo]) error {
	ctx := stream.Context()
	start := time.Now()
	method := "ListTasks"

	defer func() {
		duration := time.Since(start).Seconds()
		metrics.GRPCRequestDuration.WithLabelValues(method).Observe(duration)
	}()

	log := logger.GetOrCreateLoggerFromCtx(ctx)

	// Конвертируем proto запрос в фильтр
	filter := converters.ProtoToTaskFilter(req)

	// Получаем список задач через service
	tasks, err := h.service.ListTasks(ctx, filter)
	if err != nil {
		metrics.GRPCRequestsTotal.WithLabelValues(method, "error").Inc()
		metrics.GRPCRequestErrors.WithLabelValues(method, "internal_error").Inc()
		log.Error(ctx, "failed to list tasks", zap.String("user_id", req.UserId), zap.String("status_filter", req.StatusFilter), zap.Error(err))
		return status.Error(codes.Internal, fmt.Sprintf("failed to list tasks: %v", err))
	}

	// Отправляем задачи через stream
	for _, task := range tasks {
		taskInfo := converters.TaskToProtoTaskInfo(task)
		if err := stream.Send(taskInfo); err != nil {
			metrics.GRPCRequestErrors.WithLabelValues(method, "stream_error").Inc()
			log.Error(ctx, "failed to send task info via stream", zap.String("task_id", task.TaskID), zap.Error(err))
			return status.Error(codes.Internal, fmt.Sprintf("failed to send task info: %v", err))
		}
	}

	metrics.GRPCRequestsTotal.WithLabelValues(method, "success").Inc()
	return nil
}
