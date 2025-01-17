export PYTHONDONTWRITEBYTECODE=1

install:
	poetry install

lint:
	poetry run ruff check hmpps_person_match/
	
lint-fix:
	poetry run ruff check hmpps_person_match/ --fix

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

test: lint
	poetry run pytest -v

test-ci:
	poetry run pytest --junitxml=test_results/pytest-report.xml
