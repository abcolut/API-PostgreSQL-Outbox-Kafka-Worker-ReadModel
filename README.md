# Go Event-Driven Service

Учебный проект: HTTP API - PostgreSQL - Outbox - Kafka - Worker - Read-model (CQRS).

GO процесс приема заказов (PostGree + Redis) ### Go процесс формирования таблицы Order для чтения (модель CQRS); отправка сообщений в Кафка ### Go процесс взаимодействия с Кафка и реализация модели DLQ и At-least-once delivery


## Стек
- Go 1.21
- PostgreSQL 15
- Redis 7
- Kafka (Confluent 7.4)
- Prometheus + Grafana

## Паттерны и технологии

CQRS	                orders (write) vs orders_read (read)	    Разделение моделей для чтения и записи
Transactional Outbox    Outbox	orders + outbox в одной tx	        Атомарность записи и публикации события
At-least-once delivery	Relay → Kafka → Worker	                    Событие не потеряется при сбоях
Idempotency	            order_events_log                        	Дубликаты не создают эффекта
Dead Letter Queue	    order-events-dlq	                        Битые сообщения не теряются
Event Log	            order_events_log	                        Аудит 
Cache-aside	            OrderService.GetByID → Redis → orders_read	Ускорение чтения
Soft delete	            orders_read.deleted	                        Не терять связность событий

Prometheus — получили обратную связь о том, что происходит внутри.


# схема работы

┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT (Bruno, curl)                           │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │ HTTP
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         APP (cmd/app) — HTTP-сервер                         │
│                                                                             │
│  ┌─────────────┐    ┌──────────────────┐    ┌───────────────────────────┐   │
│  │   Handler   │───►│ OrderService     │───►│ OrderRepo (WRITE)         │   │
│  │ (POST/GET)  │    │ (бизнес-логика)  │    │ OutboxRepo                │   │
│  └─────────────┘    └────────┬─────────┘    └───────────┬───────────────┘   │
│                              │                          │                   │
│                              │ (транзакция)             │                   │
│                              ▼                          ▼                   │
│                    ┌──────────────────┐         ┌──────────────┐            │
│                    │ Cache (Redis)    │         │ OrdersRead   │            │
│                    │ GET/SET/DEL      │         │ Repo (READ)  │            │
│                    └──────────────────┘         └──────┬───────┘            │
└────────────────────────────────────────────────────────┼────────────────────┘
                                                         │
                              ┌──────────────────────────┘
                              │ (чтение)
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PostgreSQL                                     │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  ┌────────────┐   │
│  │   orders     │  │    outbox    │  │ order_events_log │  │orders_read │   │
│  │ (WRITE model)│  │ (события для │  │ (журнал всех     │  │(READ model)│   │
│  │              │  │   Kafka)     │  │  событий)        │  │            │   │
│  └──────┬───────┘  └──────┬───────┘  └──────────────────┘  └────▲───────┘   │
│         │                 │                                     │           │
└─────────┼─────────────────┼─────────────────────────────────────┼───────────┘
          │                 │                                     │
          │ (1) INSERT      │ (2) SELECT pending                  │ (5) UPSERT
          │ + outbox        │     FOR UPDATE SKIP LOCKED          │
          │   в одной tx    │                                     │
          ▼                 ▼                                     │  
┌─────────────────────────────────────────────────────────────────┼─────────┐
│                         RELAY (cmd/relay)                       │         │
│                                                                 │         │
│  1. ClaimBatch() ──► захватывает pending из outbox              │         │
│  2. WriteMessages() ──► публикует в Kafka                       │         │
│  3. MarkSent() ──► status='sent'                                │         │
│  4. Cleaner (раз в час) ──► DELETE FROM outbox WHERE sent       │         │
└─────────────────────────────────────┬───────────────────────────┼─────────┘
                                      │                           │
                                      ▼                           │
┌─────────────────────────────────────────────────────────────────┼─────────┐
│                              KAFKA                              │         │
│                                                                 │         │
│  ┌──────────────────────┐         ┌──────────────────────────┐  │         │
│  │  order-events        │         │  order-events-dlq        │  │         │
│  │  (основной топик)    │         │  (dead letter queue)     │  │         │
│  └──────────┬───────────┘         └──────────▲───────────────┘  │         │
│             │                                │                  │         │
└─────────────┼────────────────────────────────┼──────────────────┼─────────┘
              │                                │                  │
              │ (3) read                       │ (4) на ошибку    │
              ▼                                │                  │
┌────────────────────────────────────────── ───┼──────────────────┼────────┐
│                    WORKER (cmd/worker)       │                  │        │
│                                              │                  │        │
│  1. ReadMessage() ──► читает из order-events │                  │        │
│  2. insertEventTx() ──► order_events_log     │                  │        │
│  3. ordersReadRepo.Upsert() ─────────────────┼──────────────────┘        │
│  4. CommitMessages() ──► коммит offset       │                           │
│                                              │                           │
│  При ошибке: DLQProducer.Send() ─────────────┘                           │
└──────────────────────────────────────────────────────────────────────────┘

## Поток данных по операциям
### POST /orders (создание заказа)


Client ──► Handler ──► OrderService.Create()
                              │
                              ▼
                        BEGIN TRANSACTION
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    INSERT INTO orders              INSERT INTO outbox
    (WRITE model)                   (status='pending',
                                     event_type='OrderCreated')
              │                               │
              └───────────────┬───────────────┘
                              ▼
                        COMMIT TRANSACTION
                              │
                              ▼
                      HTTP 201 Created
                              │
                              ▼ (асинхронно, через relay)
    ┌─────────────────────────────────────────────────────────┐
    │  RELAY: ClaimBatch → WriteMessages → MarkSent           │
    └─────────────────────────────────────────────────────────┘
                              │
                              ▼
                        Kafka: order-events
                              │
                              ▼ (асинхронно, через worker)
    ┌─────────────────────────────────────────────────────────┐
    │  WORKER: ReadMessage → insertEventTx → Upsert orders_read│
    └─────────────────────────────────────────────────────────┘

HTTP-ответ возвращается до того, как событие уйдёт в Kafka. Гарантия — в outbox: если relay упадёт, событие останется в outbox со status='pending' и будет опубликовано при следующем цикле.

###  GET /orders/{id} (чтение заказа)

Client ──► Handler ──► OrderService.GetByID()
                              │
                              ▼
                      ┌───────────────┐
                      │  Redis cache  │
                      │  GET order:id │
                      └───────┬───────┘
                              │
                   ┌──────────┴──────────┐
                   ▼ hit                 ▼ miss
             Вернуть из кэша        orders_read
                                   (READ model)
                                        │
                                        ▼
                                ┌───────────────┐
                                │ найден?       │
                                └───┬───────┬───┘
                                    │ да    │ нет
                                    ▼       ▼
                            SET в Redis   404 Not Found
                                    │
                                    ▼
                              Вернуть клиенту

Ключевое: GET никогда не обращается к orders (WRITE model). Только к кэшу и orders_read. Это и есть разделение CQRS.
### PATCH /orders/{id} (обновление)
text

Client ──► Handler ──► OrderService.Update()
                              │
                              ▼
                        BEGIN TRANSACTION
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    UPDATE orders                   INSERT INTO outbox
    (WRITE model)                   (event_type='OrderUpdated')
              │                               │
              └───────────────┬───────────────┘
                              ▼
                        COMMIT TRANSACTION
                              │
                              ▼
                      DEL order:id (кэш)
                              │
                              ▼
                      HTTP 200 OK
                              │
                              ▼ (асинхронно)
                     Kafka → Worker → UPSERT orders_read

### DELETE /orders/{id}
text

Client ──► Handler ──► OrderService.Delete()
                              │
                              ▼
                        BEGIN TRANSACTION
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    DELETE FROM orders              INSERT INTO outbox
    (WRITE model)                   (event_type='OrderDeleted')
              │                               │
              └───────────────┬───────────────┘
                              ▼
                        COMMIT TRANSACTION
                              │
                              ▼
                      DEL order:id (кэш)
                              │
                              ▼
                      HTTP 204 No Content
                              │
                              ▼ (асинхронно)
                     Kafka → Worker → UPDATE orders_read SET deleted=TRUE







## Быстрый старт

1. Поднять инфраструктуру:
   docker-compose up -d

2. Применить миграции (если БД пустая):
   Get-Content .\migrations\*.sql | docker exec -i postgres psql -U user -d appdb

3. Запустить сервисы (в трёх терминалах):
   go run ./cmd/app
   go run ./cmd/relay
   go run ./cmd/worker

4. Проверить:
   curl.exe -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d '{"user_id":1,"item":"book","quantity":2}'
   curl.exe http://localhost:8080/orders/1

## Метрики
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## Что внутри
- `cmd/app` — HTTP API
- `cmd/relay` — outbox → Kafka
- `cmd/worker` — Kafka → read-model
- `internal/` — домен, сервисы, репозитории, кэш, события

## TODO 
- GET может не сразу увидеть только что созданный заказ.
- Нет retry-топика, только DLQ.
- Тесты минимальные.


# Система приема заказов Redis & Postgree + брокер событий на Kafka

    - Система регистрация заказов - PostGree
    - Кэширование данных - Redis
    - Создание событий в Kafka 
    - Создание worker Kafka - сервис который обрабатывает заказ (например, посылает письмо клиенту)

    ┌─────────────────────────────────────────┐
    │  Windows / VS Code                      │
    │                                         │
    │   go run ./cmd/app  ──►  localhost:8080 │
    │         │                               │
    │         │ localhost:5432                │
    │         │ localhost:6379                │
    │         │ localhost:29092 (Kafka)       │
    └─────────┼───────────────────────────────┘
              │
              ▼
    ┌─────────────────────────────────────────┐
    │  Docker Desktop                         │
    │                                         │
    │  postgres    (5432)  ── healthy         │
    │  redis       (6379)  ── healthy         │
    │  zookeeper   (2181)  ── healthy         │
    │  kafka       (9092, 29092) ── healthy   │
    └─────────────────────────────────────────┘



## проверка работоспособности системы локально
    docker-compose up -d --build
    go mod tydi
    air

    лог системы при запуске

    2026/10/27 18:06:43 PostgreSQL connected
    2026/10/27 18:06:43 Redis connected
    2026/10/27 18:06:43 Kafka producer initialized (brokers=localhost:29092, topic=order-events)
    2026/10/27 18:06:43 Server starting on :8080

    curl http://localhost:8080/health


## проверка работоспособности Систем в Докере  
    curl http://localhost:8080/health # проверка эндпоинт health (OK)
    docker exec -it redis redis-cli ping # проверка Redis (PONG)
    docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list # проверка Kafka 

## Команды докера

    Логи всех сервисов	docker-compose logs -f
    Логи конкретного	docker-compose logs -f app
    Зайти в контейнер	docker exec -it go-app sh
    Перезапустить один сервис	docker-compose restart app
    Остановить, сохранив данные	docker-compose stop
    Запустить снова	docker-compose start
    Остановить + удалить контейнеры	docker-compose down
    Удалить всё вместе с данными (volume'ы!)	docker-compose down -v


## Иницаиализация БД

    Get-Content .\migrations\001_init.sql | docker exec -i postgres psql -U user -d appdb
    Get-Content .\migrations\002_init.sql | docker exec -i postgres psql -U user -d appdb
    Get-Content .\migrations\003_outbox | docker exec -i postgres psql -U user -d appdb
    Get-Content .\migrations\004_orders_read.sql | docker exec -i postgres psql -U user -d appdb

    docker exec -it postgres psql -U user -d appdb -c "\d orders"  # проверка созданной таблицы

    docker-compose down -v      
    docker-compose up -d


## Работа с таблицей заказов
    air  - клиент
    go run ./cmd/worker - потребитель сигналов
    go run ./cmd/relay - посредник между клиентом и БД

    Создание заказа - http://localhost:8080 (method POST) - {"user_id":4,"item": "book3", "quantity":1}
    Получение заказа - http://localhost:8080/orders/2 (GET)
    Список заказов - http://localhost:8080/orders (GET)
    Изменение заказа http://localhost:8080 (method PATCH) - {"user_id":4,"item": "book3", "quantity":1}

    Создать заказ из термилала
    Invoke-RestMethod -Uri http://localhost:8080/orders -Method Post `
        -ContentType "application/json" `
        -Body '{"user_id":42,"item":"Some item","quantity":3}'

    curl.exe -X POST http://localhost:8080/orders `
        -H "Content-Type: application/json" `
        -d '{\"user_id\":1,\"item\":\"book\",\"quantity\":2}'

    Invoke-RestMethod http://localhost:8080/orders    

    docker exec -it postgres psql -U user -d appdb -c "SELECT * FROM orders;"  

    Invoke-RestMethod http://localhost:8080/orders
    Invoke-RestMethod "http://localhost:8080/orders?user_id=1" # только user_id=1
    Invoke-RestMethod "http://localhost:8080/orders?item=book" # поиск по подстроке в item
    Invoke-RestMethod "http://localhost:8080/orders?limit=5&offset=0" # пагинация
    Invoke-RestMethod "http://localhost:8080/orders?user_id=1&status=created&limit=10" # комбинация

## Проверка Таблицы order_events_log GO клиента 

    order_events_log создается для удовлетворения требования Кафка - одно изменение - одно обращение к Кафка
    docker exec -it postgres psql -U user -d appdb -c "SELECT * FROM order_events_log ORDER BY id;"  


## Проверка Кэша Redis
    После изменения состояния заказа информация попадает хранилище Redis и хранится там несколько секунд. После этого память освобождается и появляется сообщение в терминале

    GET /orders/{id} — сначала смотрим в кэш, при промахе идём в Postgres и кладём на 60 сек. При PATCH/DELETE — инвалидируем ключ.
    event logged: type=OrderCreated order_id=19 partition=0 offset=3
    docker exec -it redis redis-cli GET "order:5"


    2026/10/28 07:02:58 cache INVALIDATE order:1
    2026/10/28 07:03:19 cache INVALIDATE order:1
    2026/10/28 08:44:10 cache MISS order:12
    2026/10/28 08:44:10 db query took 8.3322ms
    2026/10/28 08:44:18 cache HIT  order:12
    2026/10/28 08:44:27 cache HIT  order:12


## Запуск докера и Go сервисов

    docker-compose up -d # Запустить инфраструктуру (Postgres, Redis, Kafka)
    docker-compose ps    # Все должны быть "healthy"  
    go mod tidy

    go run cmd/app/main.go  # Запустить Go-сервис локально (вне Docker)
    или 
    air 

    go run ./cmd/worker  # Запустить Worker - программу, которая как клиент взаимодействует с kafka

    Если Go-сервис запускается внутри Docker, используйте docker-compose up -d --build, но тогда в .env укажите localhost замените на имена сервисов (postgres, redis, kafka). 
    В данной реализации, в докере GO клиент не запускается


    go run ./cmd/relay - запуск процесса outbox - который гарантирует сохранение событий в случае падения кафка

## Работа с Кафка
    Изменения в таблице заказов регистрируются в очереди Кафка и журнале изменений без возможных дублей

    Список топиков	docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
    Создать топик	docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic NAME --partitions 3 --replication-factor 1
    Удалить топик	docker exec kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic NAME
    Описание топика	docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic NAME
    Читать сообщения	docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic NAME --from-beginning
    Писать сообщения	docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic NAME
    Группы консьюмеров	docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list
    Проверка живости брокера	docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:9092

    Создание топика docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic order-events --partitions 3 --replication-factor 1

### Имитация worker Kafka
    Программа читает очередь кафка и сбрасывает его
    go run ./cmd/worker 

### Работа с журналом Событий Кафка
    Сервис при создании сигнала в Кафка записывает журнал событий с условием - одно обращение в Кафка идет один раз

    docker exec -it postgres psql -U user -d appdb -c "
    SELECT id, event_type, order_id, kafka_partition, kafka_offset, received_at
    FROM order_events_log
    ORDER BY id DESC
    LIMIT 10;
    "

### Отправка ядовитого сообщения в Кафка
    docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic order-events this-is-not-json


## Запуск проекта

1. Клонируйте репозиторий:
   ```bash
   git clone <ссылка-на-твой-репозиторий>
   cd my_kafka


2.  Создайте файл конфигурации:

    ```bash
    cp .env.example .env

3. Установите зависимости:

    ```bash
    go mod tidy


## Основные команды
    Поднять инфру	docker-compose up -d
    Остановить инфру	docker-compose stop
    Запустить код	air или F5 в VS Code
    Логи Postgres	docker-compose logs -f postgres
    Зайти в psql	docker exec -it postgres psql -U user -d appdb
    Полный сброс БД	docker-compose down -v && docker-compose up -d
    Список топиков Kafka	docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list   


## TODO

    Миграции через golang-migrate
    Отдельный контейнер migrate в compose, который на старте прогоняет migrate up.
    Или отдельная папка migrations/ с файлами 000001_init.up.sql / 000001_init.down.sql.


    Структурированные логи (slog)
    Заменитm log.Printf на slog.Info("create order", "order_id", o.ID, "user_id", o.UserID). Логи в JSON-формате — их потом легко собирать в ELK/Loki.
    изучитm: log/slog, slog.With, slog.Group, request_id через middleware.


    Graceful shutdown полностью
    Сейчас у вас srv.Shutdown(ctx) — но Kafka writer и Redis client тоже надо корректно закрывать. Порядок: перестать принимать новые запросы → дождаться активных → закрыть Kafka writer (flush) → закрыть Redis → закрыть БД.
    Что изучите: context.WithTimeout, sync.WaitGroup, порядок закрытия ресурсов.    


    Health- и readiness-эндпоинты
    GET /health — сервис жив (просто 200 OK).
    GET /ready — сервис готов принимать трафик (проверяет подключения к Postgres/Redis/Kafka).

    Prometheus + Grafana


## Если заказы повисли после простоя worker и relay

    docker exec -it postgres psql -U user -d appdb -c "UPDATE outbox SET status = 'pending', attempts = 0 WHERE status = 'failed';"