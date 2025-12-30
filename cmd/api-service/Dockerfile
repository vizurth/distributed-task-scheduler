# syntax=docker/dockerfile:1.4

############################
#       BUILDER (glibc)
############################
FROM golang:1.25-bookworm AS builder

WORKDIR /build

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libc6-dev librdkafka-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Копируем мод-файлы
COPY go.mod go.sum ./

# Кэшируем зависимости Go
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Копируем весь проект
COPY . .

# Сборка (CGO + amd64)
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 \
    go build -o api-service ./cmd/api-service/main.go


############################
#       FINAL IMAGE
############################
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /build/api-service .
COPY --from=builder /build/configs ./configs
COPY --from=builder /build/migrations ./migrations

CMD ["./api-service"]