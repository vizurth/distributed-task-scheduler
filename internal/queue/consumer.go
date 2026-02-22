package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"go.uber.org/zap"
)

const (
	// Таймаут для чтения сообщений из Kafka (в миллисекундах, -1 = бесконечно)
	readTimeoutMs = -1
)

// Message представляет сообщение из Kafka
type Message struct {
	Type  string      // "task", "result"
	Data  interface{} // TaskMessage или ResultMessage
	Topic string
}

// Consumer интерфейс для чтения сообщений из Kafka
type Consumer interface {
	Read(ctx context.Context) (*Message, error)
	Close() error
}

// kafkaConsumer реализация Consumer для Kafka
type kafkaConsumer struct {
	consumer *kafka.Consumer
	config   *Config
	topic    string
	log      *logger.Logger
}

// NewConsumer создает новый Kafka консьюмер
func NewConsumer(config *Config, topic string) (Consumer, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers specified")
	}

	if topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}

	if config.GroupID == "" {
		return nil, fmt.Errorf("group_id is empty")
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers":       strings.Join(config.Brokers, ","),
		"group.id":                config.GroupID,
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 5000,
		"session.timeout.ms":      30000,
	}

	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	log := logger.GetOrCreateLoggerFromCtx(context.Background())
	log.Info(context.Background(), "kafka consumer created successfully",
		zap.String("topic", topic),
		zap.String("group_id", config.GroupID))

	return &kafkaConsumer{
		consumer: c,
		config:   config,
		topic:    topic,
		log:      log,
	}, nil
}

// Read читает сообщение из Kafka
func (c *kafkaConsumer) Read(ctx context.Context) (*Message, error) {
	// Проверяем контекст перед чтением
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	kafkaMsg, err := c.consumer.ReadMessage(readTimeoutMs)
	if err != nil {
		// Проверяем тип ошибки
		kafkaErr, ok := err.(kafka.Error)
		if ok && kafkaErr.Code() == kafka.ErrTimedOut {
			return nil, fmt.Errorf("timeout reading message: %w", err)
		}

		c.log.Error(ctx, "failed to read message from kafka",
			zap.String("topic", c.topic),
			zap.Error(err))
		return nil, fmt.Errorf("failed to read kafka message: %w", err)
	}

	if kafkaMsg == nil {
		return nil, fmt.Errorf("received nil kafka message")
	}

	topic := *kafkaMsg.TopicPartition.Topic
	var msgType string
	var data interface{}

	c.log.Debug(ctx, "received kafka message",
		zap.String("topic", topic),
		zap.Int32("partition", kafkaMsg.TopicPartition.Partition),
		zap.Int64("offset", int64(kafkaMsg.TopicPartition.Offset)),
		zap.Int("size", len(kafkaMsg.Value)))

	// Парсим сообщение в зависимости от топика
	if topic == c.config.TasksNewTopic {
		var task models.KafkaTaskMessage
		if err := json.Unmarshal(kafkaMsg.Value, &task); err != nil {
			c.log.Error(ctx, "failed to unmarshal task message",
				zap.String("topic", topic),
				zap.Error(err))
			return nil, fmt.Errorf("failed to unmarshal task message: %w", err)
		}

		// Валидация задачи
		if task.TaskID == "" {
			c.log.Warn(ctx, "received task with empty task_id", zap.String("topic", topic))
			return nil, fmt.Errorf("task_id is empty")
		}

		msgType = "task"
		data = &task

	} else if topic == c.config.TasksResultsTopic {
		var result models.KafkaResultMessage
		if err := json.Unmarshal(kafkaMsg.Value, &result); err != nil {
			c.log.Error(ctx, "failed to unmarshal result message",
				zap.String("topic", topic),
				zap.Error(err))
			return nil, fmt.Errorf("failed to unmarshal result message: %w", err)
		}

		// Валидация результата
		if result.TaskID == "" {
			c.log.Warn(ctx, "received result with empty task_id", zap.String("topic", topic))
			return nil, fmt.Errorf("task_id is empty in result")
		}

		msgType = "result"
		data = &result
	} else {
		c.log.Warn(ctx, "received message from unknown topic", zap.String("topic", topic))
		return nil, fmt.Errorf("unknown topic: %s", topic)
	}

	return &Message{
		Type:  msgType,
		Data:  data,
		Topic: topic,
	}, nil
}

// Close закрывает консьюмер
func (c *kafkaConsumer) Close() error {
	c.log.Info(context.Background(), "closing kafka consumer",
		zap.String("topic", c.topic),
		zap.String("group_id", c.config.GroupID))

	if err := c.consumer.Close(); err != nil {
		c.log.Error(context.Background(), "failed to close kafka consumer",
			zap.Error(err))
		return fmt.Errorf("failed to close kafka consumer: %w", err)
	}

	c.log.Info(context.Background(), "kafka consumer closed successfully")
	return nil
}
