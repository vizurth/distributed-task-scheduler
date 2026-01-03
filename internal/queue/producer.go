package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/vizurth/distributed-task-scheduler/internal/logger"
	"github.com/vizurth/distributed-task-scheduler/internal/metrics"
	"github.com/vizurth/distributed-task-scheduler/internal/models"
	"go.uber.org/zap"
)

type Producer interface {
	SendTask(msg *models.KafkaTaskMessage) error
	SendResult(msg *models.KafkaResultMessage) error
	Close()
}

type kafkaProducer struct {
	producer        *kafka.Producer
	config          *Config
	deliveryChannel chan kafka.Event
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

func NewProducer(config *Config) (Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),
		"compression.type":  config.CompressionType,
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	producer := &kafkaProducer{
		producer:        p,
		config:          config,
		deliveryChannel: make(chan kafka.Event, 500000),
		stopChan:        make(chan struct{}),
	}

	// Запускаем обработчик доставок
	producer.wg.Add(1)
	go producer.handleDeliveries()

	return producer, nil
}

func (p *kafkaProducer) SendTask(msg *models.KafkaTaskMessage) error {
	return p.sendMessage(msg, p.config.TasksNewTopic)
}

func (p *kafkaProducer) SendResult(msg *models.KafkaResultMessage) error {
	return p.sendMessage(msg, p.config.TasksResultsTopic)
}

func (p *kafkaProducer) sendMessage(message interface{}, topic string) error {
	bytes, err := json.Marshal(message)
	if err != nil {
		metrics.KafkaOperationTotal.WithLabelValues("send", "error").Inc()
		return err
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

	// АСИНХРОННАЯ отправка - возвращаемся сразу
	if err := p.producer.Produce(kafkaMsg, p.deliveryChannel); err != nil {
		metrics.KafkaOperationTotal.WithLabelValues("produce", "error").Inc()
		return fmt.Errorf("produce failed: %w", err)
	}

	metrics.KafkaOperationTotal.WithLabelValues("produce", "success").Inc()
	return nil
}

func (p *kafkaProducer) handleDeliveries() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopChan:
			return
		case event := <-p.deliveryChannel:
			m := event.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				metrics.KafkaOperationTotal.WithLabelValues("delivery", "error").Inc()
				logger.GetOrCreateLoggerFromCtx(context.Background()).Error(
					context.Background(),
					"kafka delivery failed",
					zap.Error(m.TopicPartition.Error),
				)
			} else {
				metrics.KafkaOperationTotal.WithLabelValues("delivery", "success").Inc()
				metrics.KafkaMessagesSent.WithLabelValues(*m.TopicPartition.Topic).Inc()
			}
		}
	}
}

func (p *kafkaProducer) getMessageKey(message interface{}) string {
	switch msg := message.(type) {
	case *models.KafkaTaskMessage:
		return msg.TaskID
	case *models.KafkaResultMessage:
		return msg.TaskID
	default:
		return "default"
	}
}

func (p *kafkaProducer) Close() {
	close(p.stopChan)
	p.wg.Wait()
	p.producer.Flush(30000)
	p.producer.Close()
}
