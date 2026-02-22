package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	processpb "gitlab.com/vizurth/protos/gen/go/task/task-processor"
)

// TaskExecutor отвечает за выполнение различных типов задач
type TaskExecutor struct {
	workerID string
}

// NewTaskExecutor создает новый экземпляр TaskExecutor
func NewTaskExecutor(workerID string) *TaskExecutor {
	return &TaskExecutor{
		workerID: workerID,
	}
}

// ExecuteTask выполняет задачу в зависимости от её типа
func (e *TaskExecutor) ExecuteTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	startTime := time.Now()
	defer func() {
		metrics.WorkerProcessingTasks.WithLabelValues(e.workerID).Dec()
	}()

	metrics.WorkerProcessingTasks.WithLabelValues(e.workerID).Inc()

	var result []byte
	var err error

	// Выбираем обработчик в зависимости от типа задачи
	switch models.TaskType(task.TaskType) {
	case models.TaskTypeEmail:
		result, err = e.handleEmailTask(ctx, task)
	case models.TaskTypeImage:
		result, err = e.handleImageTask(ctx, task)
	case models.TaskTypeExport:
		result, err = e.handleExportTask(ctx, task)
	default:
		err = fmt.Errorf("unknown task type: %s", task.TaskType)
	}

	// Записываем метрики длительности выполнения
	duration := time.Since(startTime).Seconds()
	metrics.WorkerTaskExecutionDuration.WithLabelValues(e.workerID, task.TaskType).Observe(duration)

	if err != nil {
		metrics.WorkerTasksExecuted.WithLabelValues(e.workerID, task.TaskType, "failed").Inc()
		return nil, err
	}

	metrics.WorkerTasksExecuted.WithLabelValues(e.workerID, task.TaskType, "success").Inc()

	return result, nil
}

// EmailTaskData представляет структуру данных для email задачи
type EmailTaskData struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// EmailTaskResult представляет результат выполнения email задачи
type EmailTaskResult struct {
	Status    string `json:"status"`
	MessageID string `json:"message_id"`
	Recipient string `json:"recipient"`
	SentAt    string `json:"sent_at"`
}

// handleEmailTask обрабатывает задачу отправки email
func (e *TaskExecutor) handleEmailTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	var emailTaskData EmailTaskData
	if err := json.Unmarshal(task.Payload, &emailTaskData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal email task payload: %w", err)
	}

	// Проверяем контекст на отмену
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
	default:
	}

	// Симулируем отправку email
	// TODO: Интегрировать с реальным email сервисом (SMTP, SendGrid, AWS SES и т.д.)
	time.Sleep(2 * time.Second)

	result := EmailTaskResult{
		Status:    "sent",
		MessageID: fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		Recipient: emailTaskData.To,
		SentAt:    time.Now().Format(time.RFC3339),
	}

	return json.Marshal(result)
}

// ImageTaskData представляет структуру данных для image задачи
type ImageTaskData struct {
	ImageURL   string `json:"image_url"`
	Operation  string `json:"operation"` // resize, crop, filter, etc.
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	Quality    int    `json:"quality,omitempty"`
	Format     string `json:"format,omitempty"`      // jpg, png, webp
	FilterType string `json:"filter_type,omitempty"` // grayscale, blur, sharpen
}

// ImageTaskResult представляет результат обработки изображения
type ImageTaskResult struct {
	Status       string `json:"status"`
	ProcessedURL string `json:"processed_url"`
	OriginalURL  string `json:"original_url"`
	Operation    string `json:"operation"`
	ProcessedAt  string `json:"processed_at"`
	FileSize     int64  `json:"file_size,omitempty"`
}

// handleImageTask обрабатывает задачу обработки изображений
func (e *TaskExecutor) handleImageTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	var imageTaskData ImageTaskData
	if err := json.Unmarshal(task.Payload, &imageTaskData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal image task payload: %w", err)
	}

	// Проверяем контекст на отмену
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
	default:
	}

	// Валидация входных данных
	if imageTaskData.ImageURL == "" {
		return nil, fmt.Errorf("image_url is required")
	}
	if imageTaskData.Operation == "" {
		return nil, fmt.Errorf("operation is required")
	}

	// Симулируем обработку изображения
	// TODO: Интегрировать с библиотекой обработки изображений (imagemagick, libvips и т.д.)
	processingTime := time.Duration(3+imageTaskData.Width/100) * time.Second

	select {
	case <-time.After(processingTime):
		// Обработка завершена
	case <-ctx.Done():
		return nil, fmt.Errorf("task cancelled during processing: %w", ctx.Err())
	}

	result := ImageTaskResult{
		Status:       "processed",
		ProcessedURL: fmt.Sprintf("https://storage.example.com/processed/%d.%s", time.Now().UnixNano(), imageTaskData.Format),
		OriginalURL:  imageTaskData.ImageURL,
		Operation:    imageTaskData.Operation,
		ProcessedAt:  time.Now().Format(time.RFC3339),
		FileSize:     int64(1024 * 512), // Примерный размер файла
	}

	return json.Marshal(result)
}

// ExportTaskData представляет структуру данных для export задачи
type ExportTaskData struct {
	Format    string                 `json:"format"`    // csv, json, xlsx, pdf
	DataType  string                 `json:"data_type"` // users, transactions, reports
	Filters   map[string]interface{} `json:"filters,omitempty"`
	StartDate string                 `json:"start_date,omitempty"`
	EndDate   string                 `json:"end_date,omitempty"`
	UserID    string                 `json:"user_id,omitempty"`
}

// ExportTaskResult представляет результат экспорта данных
type ExportTaskResult struct {
	Status      string `json:"status"`
	DownloadURL string `json:"download_url"`
	Format      string `json:"format"`
	DataType    string `json:"data_type"`
	RecordCount int    `json:"record_count"`
	FileSize    int64  `json:"file_size"`
	ExportedAt  string `json:"exported_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
}

// handleExportTask обрабатывает задачу экспорта данных
func (e *TaskExecutor) handleExportTask(ctx context.Context, task *processpb.TaskAssignment) ([]byte, error) {
	var exportTaskData ExportTaskData
	if err := json.Unmarshal(task.Payload, &exportTaskData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal export task payload: %w", err)
	}

	// Проверяем контекст на отмену
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("task cancelled: %w", ctx.Err())
	default:
	}

	// Валидация входных данных
	if exportTaskData.Format == "" {
		return nil, fmt.Errorf("format is required")
	}
	if exportTaskData.DataType == "" {
		return nil, fmt.Errorf("data_type is required")
	}

	// Валидация формата
	validFormats := map[string]bool{"csv": true, "json": true, "xlsx": true, "pdf": true}
	if !validFormats[exportTaskData.Format] {
		return nil, fmt.Errorf("invalid format: %s, supported formats: csv, json, xlsx, pdf", exportTaskData.Format)
	}

	// Симулируем экспорт данных
	// TODO: Интегрировать с реальной логикой экспорта из базы данных
	exportTime := 5 * time.Second

	select {
	case <-time.After(exportTime):
		// Экспорт завершен
	case <-ctx.Done():
		return nil, fmt.Errorf("task cancelled during export: %w", ctx.Err())
	}

	// Генерируем результат
	now := time.Now()
	expiresAt := now.Add(24 * time.Hour) // Ссылка действительна 24 часа

	result := ExportTaskResult{
		Status:      "completed",
		DownloadURL: fmt.Sprintf("https://storage.example.com/exports/%d.%s", now.UnixNano(), exportTaskData.Format),
		Format:      exportTaskData.Format,
		DataType:    exportTaskData.DataType,
		RecordCount: 1500,                   // Примерное количество записей
		FileSize:    int64(1024 * 1024 * 2), // 2MB
		ExportedAt:  now.Format(time.RFC3339),
		ExpiresAt:   expiresAt.Format(time.RFC3339),
	}

	return json.Marshal(result)
}
