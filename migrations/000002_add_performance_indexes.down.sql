-- Удаление индексов производительности

DROP INDEX IF EXISTS idx_tasks_user_id;
DROP INDEX IF EXISTS idx_tasks_created_at_desc;
DROP INDEX IF EXISTS idx_tasks_payload_gin;
DROP INDEX IF EXISTS idx_tasks_result_gin;
DROP INDEX IF EXISTS idx_tasks_status_created;

