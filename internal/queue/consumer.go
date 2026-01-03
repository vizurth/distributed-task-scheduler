package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
)

type Message struct {
	Type  string      // "task", "result"
	Data  interface{} // TaskMessage или ResultMessage
	Topic string
}

type Consumer interface {
	Read(ctx context.Context) (*Message, error)
	Close() error
}

type kafkaConsumer struct {
	consumer *kafka.Consumer
	config   *Config
	stop     bool
}

func NewConsumer(config *Config, topic string) (Consumer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers":       strings.Join(config.Brokers, ","),
		"group.id":                config.GroupID,
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 5000,
	}

	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}

	return &kafkaConsumer{
		consumer: c,
		config:   config,
		stop:     false,
	}, nil
}

func (c *kafkaConsumer) Read(ctx context.Context) (*Message, error) {
	kafkaMsg, err := c.consumer.ReadMessage(-1)
	if err != nil {
		return nil, err
	}

	topic := *kafkaMsg.TopicPartition.Topic
	var msgType string
	var data interface{}

	// Парсим в зависимости от топики
	if topic == c.config.TasksNewTopic {
		var task models.KafkaTaskMessage
		if err := json.Unmarshal(kafkaMsg.Value, &task); err != nil {
			return nil, err
		}
		msgType = "task"
		data = &task
	} else if topic == c.config.TasksResultsTopic {
		var result models.KafkaResultMessage
		if err := json.Unmarshal(kafkaMsg.Value, &result); err != nil {
			return nil, err
		}
		msgType = "result"
		data = &result
	}

	return &Message{
		Type:  msgType,
		Data:  data,
		Topic: topic,
	}, nil
}

func (c *kafkaConsumer) Close() error {
	return c.consumer.Close()
}
