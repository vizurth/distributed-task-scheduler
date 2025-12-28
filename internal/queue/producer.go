package queue

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TaskMessage struct {
	TaskID     string      `json:"task_id"`
	TaskType   string      `json:"task_type"`
	Payload    interface{} `json:"payload"`
	Priority   int         `json:"priority"`
	DeadlineMs int64       `json:"deadline_ms"`
	UserID     string      `json:"user_id"`
}

type ResultMessage struct {
	TaskID          string      `json:"task_id"`
	WorkerID        string      `json:"worker_id"`
	Status          string      `json:"status"`
	Result          interface{} `json:"result,omitempty"`
	Error           string      `json:"error,omitempty"`
	ExecutionTimeMs int64       `json:"execution_time_ms"`
}

type Producer struct {
	producer *kafka.Producer
	config   *Config
}

func NewProducer(config *Config) (*Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),
		"compression.type":  config.CompressionType,
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer: p,
		config:   config,
	}, nil
}

func (p *Producer) SendTask(msg *TaskMessage) error {
	return p.send(msg, p.config.TasksNewTopic, msg.TaskID)
}

func (p *Producer) SendResult(msg *ResultMessage) error {
	return p.send(msg, p.config.TasksResultsTopic, msg.TaskID)
}

func (p *Producer) send(message interface{}, topic, key string) error {
	bytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: bytes,
		Key:   []byte(key),
	}

	deliveryChan := make(chan kafka.Event)
	err = p.producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
	}

	return nil
}

func (p *Producer) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
}
