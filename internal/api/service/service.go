package service

import (
	"github.com/vizurth/distributed-task-scheduler/internal/api/repository"
	"github.com/vizurth/distributed-task-scheduler/internal/queue"
)

type serviceImpl struct {
	repo     repository.Repository
	producer *queue.Producer
}

func NewService(repo repository.Repository, producer *queue.Producer) Service {
	// Initialize and return a new Service instance
	return &serviceImpl{
		repo:     repo,
		producer: producer,
	}
}
