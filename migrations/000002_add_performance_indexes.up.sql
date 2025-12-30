-- Дополнительные индексы для оптимизации производительности

-- PRIMARY KEY на id уже создает индекс автоматически, поэтому отдельный индекс не нужен

-- Индекс для быстрого поиска по user_id (уже есть составной, но добавим отдельный для простых запросов)
CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id);

-- Индекс для сортировки по created_at (уже есть, но убедимся)
CREATE INDEX IF NOT EXISTS idx_tasks_created_at_desc ON tasks(created_at DESC);

-- GIN индекс для JSONB полей для быстрого поиска внутри JSON
CREATE INDEX IF NOT EXISTS idx_tasks_payload_gin ON tasks USING GIN (payload);
CREATE INDEX IF NOT EXISTS idx_tasks_result_gin ON tasks USING GIN (result);

-- Индекс для фильтрации по статусу и сортировки (композитный)
CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks(status, created_at DESC);

