.PHONY: up down test test-race migrate run

up:
	docker compose up -d

down:
	docker compose down

test:
	go test ./...

test-race:
	go test -race ./...

# Runs migrations against DATABASE_URL by booting the entrypoint, which calls store.Migrate().
migrate:
	go run ./cmd/fuelpos

run:
	go run ./cmd/fuelpos
