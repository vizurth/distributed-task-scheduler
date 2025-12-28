package queue

type Config struct {
	Brokers []string `yaml:"brokers"`

	// Topics
	TasksNewTopic     string
	TasksResultsTopic string
	TasksDLQTopic     string

	// Consumer
	GroupID string

	// Producer
	CompressionType string
}

func NewConfig(brokers []string) *Config {
	return &Config{
		Brokers:           brokers,
		TasksNewTopic:     "tasks-new",
		TasksResultsTopic: "tasks-results",
		TasksDLQTopic:     "tasks-dlq",
		GroupID:           "task-processor-group",
		CompressionType:   "snappy",
	}
}
