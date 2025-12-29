package handler

import (
	"context"
	"fmt"

	"github.com/vizurth/distributed-task-scheduler/internal/api/converters"
	"github.com/vizurth/distributed-task-scheduler/internal/api/service"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
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
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "metadata not found")
	}

	userIDs := md.Get("user_id")
	if len(userIDs) == 0 || userIDs[0] == "" {
		return "", status.Error(codes.Unauthenticated, "user_id not found in metadata")
	}

	return userIDs[0], nil
}

func (h *handlerImpl) SubmitTask(ctx context.Context, req *taskpb.SubmitTaskRequest) (*taskpb.SubmitTaskResponse, error) {
	// Извлекаем user_id из контекста
	userID, err := getUserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Конвертируем proto запрос в внутреннюю модель
	taskCreate := converters.ProtoToTaskCreate(req, userID)

	// Создаем задачу через service
	task, err := h.service.SubmitTask(ctx, taskCreate)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to submit task: %v", err))
	}

	// Конвертируем результат в proto ответ
	return converters.TaskToProtoSubmitResponse(task), nil
}

func (h *handlerImpl) GetTaskStatus(ctx context.Context, req *taskpb.GetTaskStatusRequest) (*taskpb.GetTaskStatusResponse, error) {
	// Получаем статус задачи через service
	task, err := h.service.GetTaskStatus(ctx, req.TaskId)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get task status: %v", err))
	}

	if task == nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	// Конвертируем результат в proto ответ
	return converters.TaskToProtoStatusResponse(task), nil
}

func (h *handlerImpl) CancelTask(ctx context.Context, req *taskpb.CancelTaskRequest) (*taskpb.CancelTaskResponse, error) {
	// Отменяем задачу через service
	task, err := h.service.CancelTask(ctx, req.TaskId)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to cancel task: %v", err))
	}

	if task == nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	// Проверяем, что задача действительно отменена
	success := task.Status == models.TaskStatusCancelled

	// Конвертируем результат в proto ответ
	return converters.TaskToProtoCancelResponse(task, success), nil
}

func (h *handlerImpl) ListTasks(req *taskpb.ListTasksRequest, stream grpc.ServerStreamingServer[taskpb.TaskInfo]) error {
	ctx := stream.Context()

	// Конвертируем proto запрос в фильтр
	filter := converters.ProtoToTaskFilter(req)

	// Получаем список задач через service
	tasks, err := h.service.ListTasks(ctx, filter)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to list tasks: %v", err))
	}

	// Отправляем задачи через stream
	for _, task := range tasks {
		taskInfo := converters.TaskToProtoTaskInfo(task)
		if err := stream.Send(taskInfo); err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to send task info: %v", err))
		}
	}

	return nil
}
