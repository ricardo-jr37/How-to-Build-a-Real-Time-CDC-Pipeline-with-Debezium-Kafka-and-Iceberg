# Convenience targets for the CDC lab. `make up` is the whole demo.

SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help build up down clean connectors init restart-connect status query q sql psql load load-native topics consume logs ui

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build the Kafka Connect image (Debezium + Iceberg sink)
	$(COMPOSE) build connect

up: build ## Start everything: create the lakehouse, then register both connectors
	@set -a; . ./.env; set +a; \
	if [ "$$CDC_CONVERTER" = "io.confluent.connect.avro.AvroConverter" ]; then \
		echo "CDC_CONVERTER=avro -- starting Schema Registry too"; \
		$(COMPOSE) --profile avro up -d; \
	else \
		$(COMPOSE) up -d; \
	fi
	./scripts/wait-for-stack.sh
	./scripts/init-lakehouse.sh
	./scripts/register-connectors.sh
	@$(MAKE) --no-print-directory ui

down: ## Stop the stack, keep the data
	@# --profile avro so Schema Registry is torn down too, whatever mode you are in
	$(COMPOSE) --profile avro down

clean: ## Stop the stack and delete all volumes
	$(COMPOSE) --profile avro down -v --remove-orphans

connectors: ## (Re)register the Debezium and Iceberg connectors
	./scripts/register-connectors.sh

init: ## (Re)create the Iceberg namespaces, raw tables and current-state views
	./scripts/init-lakehouse.sh

restart-connect: ## Restart the Connect worker (the fix for a stuck Iceberg sink)
	$(COMPOSE) restart connect
	./scripts/wait-for-stack.sh

status: ## Replication slot, topics, connector state, commit health, lag
	./scripts/inspect-cdc.sh

query: ## Summary of the raw change log and the current-state views
	./scripts/query-iceberg.sh

q: ## Run one SQL statement:  make q SQL="SELECT ..."
	@./scripts/query-iceberg.sh "$(SQL)"

sql: ## Open an interactive Trino shell
	$(COMPOSE) exec trino trino --server localhost:8080 --catalog iceberg --schema lakehouse

psql: ## Open a psql shell on the source database
	$(COMPOSE) exec postgres psql -U postgres -d shop

load: ## Generate write load against Postgres (containerised, no local deps)
	docker run --rm --network cdc-iceberg_default \
		-v "$$PWD/scripts:/scripts:ro" \
		-e PGHOST=postgres -e PGPORT=5432 \
		python:3.12-slim bash -c \
		"pip install -q psycopg2-binary && python /scripts/generate-load.py --rate 5 --duration 120"

load-native: ## Same, using your local python (needs psycopg2-binary)
	@set -a; . ./.env; set +a; \
	python3 scripts/generate-load.py --port $$POSTGRES_PORT --rate 5 --duration 120

topics: ## List CDC topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:29092 --list

consume: ## Print CDC events from a topic (TOPIC=shop.public.orders N=1)
	@set -a; . ./.env; set +a; \
	if [ "$$CDC_CONVERTER" = "io.confluent.connect.avro.AvroConverter" ]; then \
		$(COMPOSE) exec -T schema-registry kafka-avro-console-consumer \
			--bootstrap-server kafka:29092 \
			--property schema.registry.url=http://schema-registry:8081 \
			--topic $${TOPIC:-shop.public.orders} --from-beginning --max-messages $${N:-1}; \
	else \
		$(COMPOSE) exec -T kafka kafka-console-consumer \
			--bootstrap-server localhost:29092 \
			--topic $${TOPIC:-shop.public.orders} --from-beginning --max-messages $${N:-1}; \
	fi

logs: ## Follow Kafka Connect logs
	$(COMPOSE) logs -f connect

ui: ## Print the web endpoints (ports come from .env)
	@set -a; . ./.env; set +a; \
	echo "Kafka UI        http://localhost:$$KAFKA_UI_PORT"; \
	echo "Kafka Connect   http://localhost:$$CONNECT_PORT"; \
	echo "Trino           http://localhost:$$TRINO_PORT"; \
	echo "Iceberg REST    http://localhost:$$ICEBERG_REST_PORT"; \
	echo "Schema Registry http://localhost:$$SCHEMA_REGISTRY_PORT"; \
	echo "MinIO console   http://localhost:$$MINIO_CONSOLE_PORT  ($$MINIO_ROOT_USER / $$MINIO_ROOT_PASSWORD)"; \
	echo "Kafka bootstrap localhost:$$KAFKA_PORT"; \
	echo "Postgres        localhost:$$POSTGRES_PORT  ($$POSTGRES_USER / $$POSTGRES_PASSWORD, db=$$POSTGRES_DB)"
