package handler

import (
	"context"

	"github.com/vizurth/distributed-task-scheduler/internal/api/service"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handlerImpl struct {
	taskpb.UnimplementedTaskAPIServer
	service service.Service
}

func NewHandler(service service.Service) Handler {
	// Initialize and return a new Handler instance
	return &handlerImpl{
		service: service,
	}
}

func (h *handlerImpl) SubmitTask(context.Context, *taskpb.SubmitTaskRequest) (*taskpb.SubmitTaskResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SubmitTask not implemented")
}
func (h *handlerImpl) GetTaskStatus(context.Context, *taskpb.GetTaskStatusRequest) (*taskpb.GetTaskStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetTaskStatus not implemented")
}
func (h *handlerImpl) CancelTask(context.Context, *taskpb.CancelTaskRequest) (*taskpb.CancelTaskResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CancelTask not implemented")
}
func (h *handlerImpl) ListTasks(*taskpb.ListTasksRequest, grpc.ServerStreamingServer[taskpb.TaskInfo]) error {
	return status.Error(codes.Unimplemented, "method ListTasks not implemented")
}
