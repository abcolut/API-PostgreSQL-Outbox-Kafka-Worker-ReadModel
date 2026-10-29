# ---- Этап сборки ----
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Сначала копируем go.mod/go.sum, чтобы кэшировать зависимости
COPY go.mod go.sum ./
RUN go mod download

# Теперь копируем весь код
COPY . .

# Собираем бинарник
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/app ./cmd/app

# ---- Финальный этап (маленький образ) ----
FROM alpine:3.19

WORKDIR /app

# Нужно для HTTPS-запросов и работы с PostgreSQL
RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /app/bin/app .

EXPOSE 8080

CMD ["./app"]