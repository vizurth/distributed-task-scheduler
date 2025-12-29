package repository

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type repositoryImpl struct {
	db     *pgxpool.Pool
	psql   sq.StatementBuilderType
	client *redis.Client
}

func NewRepository(db *pgxpool.Pool, client *redis.Client) Repository {
	// Initialize and return a new Repository instance
	return &repositoryImpl{
		db:     db,
		client: client,
	}
}
