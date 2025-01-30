export PYTHONDONTWRITEBYTECODE=1

install:
	uv sync

lint:
	uv run ruff check hmpps_person_match/ hmpps_cpr_splink/ integration/
	
lint-fix:
	uv run ruff check hmpps_person_match/ hmpps_cpr_splink/ integration/ --fix

format:
	uv run ruff format 

run-local:
	uv run fastapi dev asgi.py --port 5000

build:
	docker build . --tag hmpps_person_match \
		--build-arg BUILD_NUMBER="local" \
		--build-arg GIT_REF=$(shell git rev-parse --short HEAD) \
		--build-arg GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

run-docker:
	docker run -p 5000:5000 -t hmpps_person_match

start-containers:
	docker compose up -d

stop-containers:
	docker compose down

restart-containers: stop-containers start-containers

test: lint
	uv run pytest hmpps_person_match/ hmpps_cpr_splink/ -v

test-ci: lint
	uv run pytest hmpps_person_match/ hmpps_cpr_splink/ --junitxml=test_results/pytest-unit-test-report.xml

test-integration: lint
	uv run pytest integration/

test-integration-ci: lint
	uv run pytest integration/ --junitxml=test_results/pytest-integration-test-report.xml