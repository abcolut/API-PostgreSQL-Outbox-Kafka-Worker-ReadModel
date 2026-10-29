up:        ; docker-compose up -d
down:      ; docker-compose down
logs:      ; docker-compose logs -f
run:       ; air
test:      ; go test ./... -race -count=1
lint:      ; golangci-lint run
migrate-up:; docker exec -i postgres psql -U user -d appdb < migrations/001_init.sql