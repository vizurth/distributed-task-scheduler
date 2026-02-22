package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vizurth/distributed-task-scheduler/internal/constants"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"go.uber.org/zap"
)

// Producer интерфейс для отправки сообщений в Kafka
type Producer interface {
	SendTask(msg *models.KafkaTaskMessage) error
	SendResult(msg *models.KafkaResultMessage) error
	Close()
}

// kafkaProducer реализация Producer для Kafka
type kafkaProducer struct {
	producer        *kafka.Producer
	config          *Config
	deliveryChannel chan kafka.Event
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

// NewProducer создает новый Kafka продюсер
func NewProducer(config *Config) (Producer, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers specified")
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),
		"compression.type":  config.CompressionType,
		"acks":              "all", // Ждем подтверждения от всех реплик
		"retries":           3,     // Количество повторных попыток
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	producer := &kafkaProducer{
		producer:        p,
		config:          config,
		deliveryChannel: make(chan kafka.Event, constants.KafkaDeliveryChannelBuffer),
		stopChan:        make(chan struct{}),
	}

	// Запускаем обработчик подтверждений доставки
	producer.wg.Add(1)
	go producer.handleDeliveries()

	return producer, nil
}

// SendTask отправляет задачу в Kafka топик tasks_new
func (p *kafkaProducer) SendTask(msg *models.KafkaTaskMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	if msg.TaskID == "" {
		return fmt.Errorf("task_id is required")
	}

	return p.sendMessage(msg, p.config.TasksNewTopic)
}

// SendResult отправляет результат задачи в Kafka топик tasks_results
func (p *kafkaProducer) SendResult(msg *models.KafkaResultMessage) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	if msg.TaskID == "" {
		return fmt.Errorf("task_id is required")
	}

	return p.sendMessage(msg, p.config.TasksResultsTopic)
}

// sendMessage отправляет сообщение в указанный топик
func (p *kafkaProducer) sendMessage(message interface{}, topic string) error {
	if topic == "" {
		metrics.KafkaOperationTotal.WithLabelValues("send", "error").Inc()
		return fmt.Errorf("topic is empty")
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		metrics.KafkaOperationTotal.WithLabelValues("send", "marshal_error").Inc()
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	key := p.getMessageKey(message)

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: bytes,
		Key:   []byte(key),
	}

	// АСИНХРОННАЯ отправка - возвращаемся сразу после постановки в очередь
	if err := p.producer.Produce(kafkaMsg, p.deliveryChannel); err != nil {
		metrics.KafkaOperationTotal.WithLabelValues("produce", "error").Inc()
		return fmt.Errorf("failed to produce message to kafka: %w", err)
	}

	metrics.KafkaOperationTotal.WithLabelValues("produce", "success").Inc()
	return nil
}

// handleDeliveries обрабатывает подтверждения доставки сообщений от Kafka
func (p *kafkaProducer) handleDeliveries() {
	defer p.wg.Done()

	log := logger.GetOrCreateLoggerFromCtx(context.Background())

	for {
		select {
		case <-p.stopChan:
			log.Info(context.Background(), "stopping delivery handler")
			return

		case event := <-p.deliveryChannel:
			m, ok := event.(*kafka.Message)
			if !ok {
				log.Warn(context.Background(), "received non-message event in delivery channel")
				continue
			}

			if m.TopicPartition.Error != nil {
				metrics.KafkaOperationTotal.WithLabelValues("delivery", "error").Inc()
				log.Error(
					context.Background(),
					"kafka message delivery failed",
					zap.String("topic", *m.TopicPartition.Topic),
					zap.Int32("partition", m.TopicPartition.Partition),
					zap.Error(m.TopicPartition.Error),
				)
			} else {
				metrics.KafkaOperationTotal.WithLabelValues("delivery", "success").Inc()
				metrics.KafkaMessagesSent.WithLabelValues(*m.TopicPartition.Topic).Inc()

				log.Debug(
					context.Background(),
					"kafka message delivered successfully",
					zap.String("topic", *m.TopicPartition.Topic),
					zap.Int32("partition", m.TopicPartition.Partition),
					zap.Int64("offset", int64(m.TopicPartition.Offset)),
				)
			}
		}
	}
}

// getMessageKey возвращает ключ сообщения для партиционирования в Kafka
func (p *kafkaProducer) getMessageKey(message interface{}) string {
	switch msg := message.(type) {
	case *models.KafkaTaskMessage:
		if msg.TaskID != "" {
			return msg.TaskID
		}
	case *models.KafkaResultMessage:
		if msg.TaskID != "" {
			return msg.TaskID
		}
	}
	return "default"
}

// Close корректно закрывает продюсер
func (p *kafkaProducer) Close() {
	log := logger.GetOrCreateLoggerFromCtx(context.Background())
	log.Info(context.Background(), "closing kafka producer")

	// Останавливаем обработчик доставок
	close(p.stopChan)

	// Ждем завершения обработчика
	p.wg.Wait()

	// Ждем доставки всех pending сообщений (30 секунд)
	remaining := p.producer.Flush(int(constants.KafkaFlushTimeout.Milliseconds()))
	if remaining > 0 {
		log.Warn(context.Background(), "some messages were not delivered before shutdown",
			zap.Int("remaining", remaining))
	}

	// Закрываем продюсер
	p.producer.Close()

	log.Info(context.Background(), "kafka producer closed successfully")
}
