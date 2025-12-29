CREATE TABLE tasks (
    -- Идентификация
    id UUID PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,

    -- Типизация
    task_type VARCHAR(100) NOT NULL,

    -- Данные задачи
    payload JSONB NOT NULL,
    result JSONB,
    error TEXT,

    -- Статус
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Приоритизация и дедлайн
    priority INT DEFAULT 5,
    deadline_ms BIGINT,

    -- Временные метки
    created_at BIGINT NOT NULL,
    started_at BIGINT,
    completed_at BIGINT,

    -- Информация о выполнении
    worker_id VARCHAR(255),
    execution_time_ms BIGINT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,

    -- Webhook
    webhook_url TEXT,

    -- Ограничения
    CONSTRAINT check_status CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled'))
);

-- Индексы
CREATE INDEX idx_tasks_user_status ON tasks(user_id, status);
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_created ON tasks(created_at DESC);
CREATE INDEX idx_tasks_priority_deadline ON tasks(priority DESC, deadline_ms);
CREATE INDEX idx_tasks_worker ON tasks(worker_id) WHERE status = 'processing';
