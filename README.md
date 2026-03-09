# Распределённый планировщик задач

Высокопроизводительная распределённая система очередей задач для асинхронной обработки работ. Построена на gRPC, Kafka, PostgreSQL и Redis с пропускной способностью примерно 6000 RPS.

## Обзор

Масштабируемая архитектура микросервисов для управления выполнением фоновых задач на распределённых рабочих узлах. Система обеспечивает распределение задач, мониторинг выполнения, сбор метрик и корректное завершение работы.

## Архитектура

Система состоит из трёх основных компонентов:

API Service

- gRPC сервер для отправки задач и запросов статуса
- Управление задачами через API endpoints
- Endpoint метрик на порту 8001

Processor Service

- Распределение задач доступным workers
- Управление соединениями и здоровьем workers
- Управление очередью и назначение работ в зависимости от доступности workers
- Endpoint метрик на порту 8002

Worker Service

- Выполнение назначенных задач
- Передача результатов обратно в processor
- Поддержка горизонтального масштабирования

## Диаграмма архитектуры
![Alt text](design.svg)


## Технологический стек

Язык: Go 1.25.4

RPC Фреймворк: gRPC 1.78.0

Очередь сообщений: Apache Kafka 7.7.1

База данных: PostgreSQL 14

Кеш: Redis latest

Метрики: Prometheus latest

Визуализация: Grafana latest

Контейнеризация: Docker Compose

## Ключевые особенности

- Распределённая обработка задач с горизонтальным масштабированием
- Отслеживание статуса задач в реальном времени
- Автоматический мониторинг здоровья workers
- Комплексные метрики и наблюдаемость
- Корректное завершение с обработкой текущих задач
- Повторное выполнение задач и обработка ошибок
- Распределение задач по приоритету
- Connection pooling для баз данных

## Спецификации производительности

- Пропускная способность: Примерно 6000 RPS
- Типы задач: email, image, export (расширяемо)
- Слоты worker: 5 одновременных задач на worker
- Соединения БД: максимум 10, минимум 2 в пуле

## Структура проекта

````
distributed-task-scheduler/
├── cmd/
│   ├── api-service/          Точка входа API сервиса
│   ├── processor/            Точка входа Processor сервиса
│   └── worker/               Точка входа Worker сервиса
├── internal/
│   ├── api/                  Реализация API сервиса
│   │   ├── handler/          gRPC обработчики
│   │   ├── service/          Бизнес-логика
│   │   ├── repository/       Слой доступа к данным
│   │   └── converters/       DTO конвертеры
│   ├── processor/            Реализация Processor
│   │   ├── handler/          Логика распределения задач
│   │   ├── manager/          Управление workers и очередью
│   │   ├── service/          Бизнес-логика
│   │   └── repository/       Слой доступа к данным
│   ├── worker/               Реализация Worker
│   │   ├── client/           Коммуникация с processor
│   │   └── executor/         Логика выполнения задач
│   ├── models/               Модели данных
│   ├── config/               Управление конфигурацией
│   ├── logger/               Структурированное логирование
│   ├── metrics/              Метрики Prometheus
│   ├── postgres/             Соединение с БД
│   ├── redis/                Соединение с кешем
│   ├── queue/                Kafka producer и consumer
│   └── grpc/                 gRPC перехватчики
├── migrations/               Скрипты миграций БД
├── monitoring/               Конфигурация Prometheus и Grafana
├── configs/                  Конфигурационные файлы приложения
└── docker-compose.yaml       Оркестрация контейнеров

## Начало работы

### Требования

- Docker и Docker Compose
- Go 1.25.4 (для локальной разработки)
- Утилита Make

### Быстрый старт

```bash
git clone <repository-url>
cd distributed-task-scheduler

make up

docker-compose logs -f

make down
````

### Локальная сборка для разработки

```bash
make up
```

## Endpoints сервисов

API Service

- gRPC сервер: localhost:50051
- Метрики: http://localhost:8001/metrics

Processor Service

- gRPC сервер: localhost:50052
- Метрики: http://localhost:8002/metrics

## Мониторинг и наблюдаемость

### Grafana Dashboard

- URL: http://localhost:3000
- Пользователь: admin
- Пароль: admin

Предварительно настроенная панель для мониторинга производительности системы.

### Prometheus

- URL: http://localhost:9090
- Интервал сбора метрик: 15 секунд
- Период хранения: 7 дней

### Kafka UI

- URL: http://localhost:9020
- Мониторинг кластера Kafka и топиков сообщений

## Метрики

### Метрики API Service

taskscheduler_grpc_requests_total

- Общее количество gRPC запросов по методам и статусам

taskscheduler_grpc_request_duration_seconds

- Гистограмма задержки запросов

taskscheduler_grpc_request_errors_total

- Общее количество ошибок по методам и типам

taskscheduler_tasks_submitted_total

- Общее количество отправленных задач по типам

taskscheduler_tasks_by_status

- Количество задач по статусам

### Метрики Processor Service

taskscheduler_processor_tasks_distributed_total

- Распределённые задачи по типам и статусам

taskscheduler_processor_tasks_in_queue

- Задачи в ожидании распределения

taskscheduler_processor_active_workers

- Количество подключённых workers

taskscheduler_processor_task_assignment_duration_seconds

- Время распределения задачи worker

### Метрики Worker

taskscheduler_worker_tasks_executed_total

- Выполненные задачи по worker, типу и статусу

taskscheduler_worker_task_execution_duration_seconds

- Гистограмма времени выполнения задачи

taskscheduler_worker_status

- Статус здоровья worker (1 = здоров, 0 = нездоров)

taskscheduler_worker_processing_tasks

- Количество текущих обрабатываемых задач

### Метрики инфраструктуры

taskscheduler_db_operation_duration_seconds

- Задержка запросов к БД

taskscheduler_db_operation_total

- Количество операций с БД

taskscheduler_redis_operation_duration_seconds

- Задержка операций Redis

taskscheduler_kafka_operation_total

- Количество операций с очередью

## База данных

### Миграции

Миграции БД применяются автоматически при запуске сервиса. Находятся в директории migrations/.

## Очередь сообщений

Apache Kafka используется для асинхронной передачи событий задач:

- Brokers: kafka1, kafka2, kafka3
- Zookeeper: zookeeper:2181
- UI: kafka-ui:9020

## Типы задач

### Поддерживаемые типы задач

- email: Отправка email уведомлений
- image: Обработка изображений
- export: Генерация экспортов данных

Типы задач расширяемы. Новые типы можно добавлять в пакет executor.

## Спецификация API

Отправка задачи

```
rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse)
```

Метаданные запроса должны содержать заголовок user_id.

Получение статуса задачи

```
rpc GetTaskStatus(GetTaskStatusRequest) returns (GetTaskStatusResponse)
```

Возвращает текущее состояние задачи и детали выполнения.

Список задач

```
rpc ListTasks(ListTasksRequest) returns (ListTasksResponse)
```

Запрос задач с опциями фильтрации.

## Логирование

Структурированное логирование используя Uber Zap:

- Формат: JSON
- Уровень: Настраивается через код
- Вывод: Логи контейнеров Docker

Просмотр логов:

```bash
docker-compose logs -f [service-name]
```

## Тестирование

### Запуск тестов

```bash
make test
```
