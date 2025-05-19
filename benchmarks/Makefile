# --- Configurable defaults ---
JOB        ?= fast
COUNT      ?= 10000
WORKERS    ?= 4
DELAYED_REF ?= main

# --- Container configuration ---
export DELAYED_REF

# --- Container lifecycle commands ---
.PHONY: down build up-app start-workers stop-workers enqueue monitor up run full prune clean

down:
	podman-compose down --volumes --remove-orphans

build:
	podman-compose build --no-cache

up-app:
	podman-compose up -d app

start-workers:
	@echo "Spawning $(WORKERS) worker containers..."
	@seq $(WORKERS) | xargs -I{} -P$(WORKERS) podman-compose run --rm worker &
	@wait

stop-workers:
	podman ps --filter label=role=benchmark_worker --format '{{.ID}}' | xargs -r podman stop
	podman ps -a --filter label=role=benchmark_worker --format '{{.ID}}' | xargs -r podman rm

enqueue:
	podman-compose run --rm app bundle exec rake benchmark:enqueue[$(JOB),$(COUNT)]

monitor:
	podman-compose run --rm app bundle exec rake benchmark:run[$(JOB),$(WORKERS)]

up: build up-app

run: enqueue start-workers monitor stop-workers

full: down up run

prune:
	podman image prune -f

clean: down prune
