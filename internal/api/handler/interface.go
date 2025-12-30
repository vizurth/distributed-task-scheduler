package handler

import (
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
)

type Handler interface {
	taskpb.TaskAPIServer
}
