package converters

import (
	"fmt"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/constants"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	taskpb "gitlab.com/vizurth/protos/gen/go/task/task-api-service"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProtoToTaskCreate конвертирует SubmitTaskRequest в TaskCreate
func ProtoToTaskCreate(req *taskpb.SubmitTaskRequest, userID string) *models.TaskCreate {
	return &models.TaskCreate{
		TaskType:   models.TaskType(req.TaskType),
		Payload:    req.Payload,
		Priority:   req.Priority,
		DeadlineMs: req.DeadlineMs,
		WebhookURL: req.WebhookUrl,
		UserID:     userID,
	}
}

// TaskToProtoSubmitResponse конвертирует Task в SubmitTaskResponse
func TaskToProtoSubmitResponse(task *models.Task) *taskpb.SubmitTaskResponse {
	return &taskpb.SubmitTaskResponse{
		TaskId:    task.TaskID,
		Status:    string(task.Status),
		CreatedAt: timestamppb.New(task.CreatedAt),
	}
}

// TaskToProtoStatusResponse конвертирует Task в GetTaskStatusResponse
func TaskToProtoStatusResponse(task *models.Task) *taskpb.GetTaskStatusResponse {
	resp := &taskpb.GetTaskStatusResponse{
		TaskId:          task.TaskID,
		Status:          string(task.Status),
		Progress:        task.Progress,
		CreatedAt:       timestamppb.New(task.CreatedAt),
		Result:          task.Result,
		Error:           task.Error,
		ExecutionTimeMs: task.ExecutionTimeMs,
	}

	if task.StartedAt != nil {
		resp.StartedAt = timestamppb.New(*task.StartedAt)
	}

	if task.CompletedAt != nil {
		resp.CompletedAt = timestamppb.New(*task.CompletedAt)
	}

	return resp
}

// TaskToProtoCancelResponse конвертирует Task в CancelTaskResponse
func TaskToProtoCancelResponse(task *models.Task, success bool) *taskpb.CancelTaskResponse {
	return &taskpb.CancelTaskResponse{
		TaskId:  task.TaskID,
		Status:  string(task.Status),
		Success: success,
	}
}

// TaskToProtoTaskInfo конвертирует Task в TaskInfo
func TaskToProtoTaskInfo(task *models.Task) *taskpb.TaskInfo {
	info := &taskpb.TaskInfo{
		TaskId:     task.TaskID,
		TaskType:   string(task.TaskType),
		Status:     string(task.Status),
		Priority:   task.Priority,
		CreatedAt:  timestamppb.New(task.CreatedAt),
		DeadlineMs: task.DeadlineMs,
	}

	if task.StartedAt != nil {
		info.StartedAt = timestamppb.New(*task.StartedAt)
	}

	if task.CompletedAt != nil {
		info.CompletedAt = timestamppb.New(*task.CompletedAt)
	}

	return info
}

// ProtoToTaskFilter конвертирует ListTasksRequest в TaskFilter
func ProtoToTaskFilter(req *taskpb.ListTasksRequest) *models.TaskFilter {
	filter := &models.TaskFilter{
		UserID:       req.UserId,
		Limit:        req.Limit,
		StatusFilter: req.StatusFilter,
	}

	// Конвертируем строковый статус в TaskStatus если это не "all"
	if req.StatusFilter != "all" && req.StatusFilter != "" {
		filter.Status = models.TaskStatus(req.StatusFilter)
	}

	return filter
}

// TaskToKafkaMessage конвертирует Task в KafkaTaskMessage
func TaskToKafkaMessage(task *models.Task) *models.KafkaTaskMessage {
	if task == nil {
		return nil
	}

	return &models.KafkaTaskMessage{
		TaskID:     task.TaskID,
		TaskType:   string(task.TaskType),
		Payload:    task.Payload,
		Priority:   task.Priority,
		DeadlineMs: task.DeadlineMs,
		UserID:     task.UserID,
	}
}

// TimestampToTime конвертирует proto Timestamp в time.Time
func TimestampToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// TimeToTimestamp конвертирует time.Time в proto Timestamp
func TimeToTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// ValidateTaskCreate проверяет корректность данных для создания задачи
func ValidateTaskCreate(task *models.TaskCreate) error {
	if task == nil {
		return fmt.Errorf("task is nil")
	}

	if task.UserID == "" {
		return fmt.Errorf(constants.ErrMsgUserIDRequired)
	}

	if task.TaskType == "" {
		return fmt.Errorf(constants.ErrMsgTaskTypeRequired)
	}

	// Проверяем что тип задачи поддерживается
	validTypes := map[models.TaskType]bool{
		models.TaskTypeEmail:  true,
		models.TaskTypeImage:  true,
		models.TaskTypeExport: true,
	}

	if !validTypes[task.TaskType] {
		return fmt.Errorf("%s: %s", constants.ErrMsgInvalidTaskType, task.TaskType)
	}

	if task.Priority < constants.MinTaskPriority || task.Priority > constants.MaxTaskPriority {
		return fmt.Errorf(constants.ErrMsgInvalidPriority)
	}

	if len(task.Payload) == 0 {
		return fmt.Errorf(constants.ErrMsgPayloadRequired)
	}

	return nil
}
