package handler

import (
	"github.com/vizurth/distributed-task-scheduler/internal/api/service"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
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
