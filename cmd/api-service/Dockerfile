# syntax=docker/dockerfile:1

FROM golang:1.21 AS builder

WORKDIR /app

# Кешируем слой с зависимостями (не переустанавливаем если go.mod не изменился)
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Кешируем компиляцию Go
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -o api-service ./cmd/api-service

# Финальный образ
FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /root/

COPY --from=builder /app/api-service .

EXPOSE 50051

CMD ["./api-service"]