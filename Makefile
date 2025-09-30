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
	docker build . --tag hmpps-person-match \
		--build-arg BUILD_NUMBER="local" \
		--build-arg GIT_REF="ref" \
		--build-arg GIT_BRANCH="branch"

run-docker:
	docker run -p 5000:5000 -t hmpps_person_match

start-containers:
	docker compose up -d

start-containers-local:
	docker compose up -d migrations hmpps-person-match

stop-containers:
	docker compose down

restart-containers: stop-containers start-containers

test: lint
	uv run pytest hmpps_person_match/ hmpps_cpr_splink/ -vvv

test-ci: lint
	uv run pytest hmpps_person_match/ hmpps_cpr_splink/ --junitxml=test_results/pytest-unit-test-report.xml

test-integration: lint start-containers
	uv run pytest integration/ -vvv

test-integration-ci: lint start-containers
	uv run pytest integration/ --junitxml=test_results/pytest-integration-test-report.xml