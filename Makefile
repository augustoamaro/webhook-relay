COMPOSE ?= docker compose -f deploy/docker-compose.yml

.PHONY: up down test lint load spike chaos reconcile

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

test:
	go test ./... -race

lint:
	golangci-lint run

load: ## steady load test against the compose stack
	k6 run -e BASE_URL=http://localhost:8080 loadtest/steady.js

spike:
	k6 run -e BASE_URL=http://localhost:8080 loadtest/spike.js

reconcile:
	$(COMPOSE) run --rm --entrypoint reconcile api

chaos: ## load + Redis FLUSHALL mid-test + reconciliation proof
	k6 run -e BASE_URL=http://localhost:8080 loadtest/steady.js & \
	K6_PID=$$!; \
	sleep 45; \
	echo ">>> CHAOS: flushing Redis mid-load"; \
	$(COMPOSE) exec redis redis-cli FLUSHALL; \
	wait $$K6_PID; \
	echo ">>> draining and reconciling"; \
	$(MAKE) reconcile
