export PYTHONDONTWRITEBYTECODE=1

install:
	poetry install

lint:
	poetry run ruff check .
	
lint-fix:
	poetry run ruff check . --fix

format:
	poetry run ruff format 

run-local:
	poetry run fastapi dev asgi.py --port 5000

build:
	docker build . --tag hmpps_person_match \
		--build-arg BUILD_NUMBER="local" \
		--build-arg GIT_REF=$(shell git rev-parse --short HEAD) \
		--build-arg GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

run-docker:
	docker run -p 5000:5000 -t hmpps_person_match

start-containers:
	docker compose up -d --build

stop-containers:
	docker compose down

restart-containers: stop-containers start-containers

test: lint
	poetry run pytest hmpps_person_match/ -v

test-ci: lint
	poetry run pytest hmpps_person_match/ --junitxml=test_results/pytest-unit-test-report.xml

test-integration: lint
	poetry run pytest integration/

test-integration-ci: lint
	poetry run pytest integration/ --junitxml=test_results/pytest-integration-test-report.xml