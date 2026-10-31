package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"my-go-service/internal/cache"
	"my-go-service/internal/config"
	"my-go-service/internal/events"
	"my-go-service/internal/handlers"
	"my-go-service/internal/ratelimit"
	"my-go-service/internal/repository/postgres"
	"my-go-service/internal/service"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	httptransport "my-go-service/internal/transport/http"
)

func main() {
	_ = godotenv.Load() // игнорируем ошибку, если .env нет
	cfg := config.Load()

	// --- PostgreSQL ---
	db, err := sql.Open("postgres", cfg.PostgresDSN())
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := waitForDB(db, 15); err != nil {
		log.Fatalf("db not ready: %v", err)
	}
	log.Println("PostgreSQL connected")

	// --- Redis ---
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("redis ping: %v", err)
	}
	cancel()
	log.Println("Redis connected")

	// --- Kafka Producer ---
	producer := events.NewOrderProducer(cfg.KafkaBrokers, "order-events")
	defer producer.Close()
	log.Printf("Kafka producer initialized (brokers=%s, topic=order-events)", cfg.KafkaBrokers)

	orderRepo := postgres.NewOrderRepo(db)
	readRepo := postgres.NewOrdersReadRepo(db)
	outboxRepo := postgres.NewOutboxRepo(db)
	orderCache := cache.NewOrderCache(rdb, 60*time.Second)
	orderSvc := service.NewOrderService(orderRepo, readRepo, orderCache, outboxRepo, db)
	orderHandler := httptransport.NewOrderHandler(orderSvc)

	// --- HTTP ---
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/api/rate-limit-test", handlers.RateLimitApiHandler)
	mux.HandleFunc("/rate-limit-test", handlers.RateLimitHtmlHandler)

	mux.HandleFunc("/api/orders", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			orderHandler.List(w, r)
		case http.MethodPost:
			orderHandler.Create(w, r)
		default:
			http.Error(w, "method not allowed for /api/orders", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/orders/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			orderHandler.GetByID(w, r)
		case http.MethodPatch, http.MethodPut:
			orderHandler.Update(w, r)
		case http.MethodDelete:
			orderHandler.Delete(w, r)
		default:
			http.Error(w, "method not allowed for /api/orders/", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/order_list", handlers.OrdersListHandler)
	mux.HandleFunc("/order_new", handlers.OrderNewHandler)
	mux.HandleFunc("/order_change", handlers.OrderChangeHandler)
	http.HandleFunc("/favicon.ico", handlers.FaviconHandler)
	mux.HandleFunc("/", handlers.IndexHandler)

	// 1 запросов в секунду, всплеск до 2
	tb := ratelimit.NewTokenBucket(5, 10)
	handlerWithLimits := ratelimit.RateLimitMiddleware(tb)(mux)

	srv := &http.Server{
		Addr: ":8080",
		// Handler: mux,
		Handler: handlerWithLimits, // используем обёрнутый хендлер
	}
	go func() {
		log.Println("Server starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server starting on :9091")
		if err := http.ListenAndServe(":9091", mux); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// --- Graceful shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

func waitForDB(db *sql.DB, attempts int) error {
	for i := 0; i < attempts; i++ {
		if err := db.Ping(); err == nil {
			return nil
		}
		log.Printf("waiting for db... (%d/%d)", i+1, attempts)
		time.Sleep(2 * time.Second)
	}
	return db.Ping()
}
