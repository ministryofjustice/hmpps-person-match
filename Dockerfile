# syntax=docker/dockerfile:1
FROM python:3.12.10-slim-bullseye AS base

# load in build details
ARG BUILD_NUMBER
ARG GIT_REF
ARG GIT_BRANCH

ENV TZ=Europe/London

ENV APP_BUILD_NUMBER=${BUILD_NUMBER} \
    APP_GIT_REF=${GIT_REF} \
    APP_GIT_BRANCH=${GIT_BRANCH}

RUN apt-get update \
    && apt-get -y upgrade 

# Update pip
RUN pip install --upgrade pip

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # uv args
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

##############
# BUILD stage
##############
FROM base AS build
COPY --from=ghcr.io/astral-sh/uv:0.7.8 /uv /bin/uv

RUN apt-get update && apt-get install --no-install-recommends -y \
        # deps for installing uv
        curl \
        # deps for building python deps
        build-essential

WORKDIR /app

COPY uv.lock pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY ./hmpps_person_match /app/hmpps_person_match/
COPY ./hmpps_cpr_splink /app/hmpps_cpr_splink/
COPY docker-entrypoint.sh asgi.py alembic.ini /app/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Upgrade pip inside the virtual environment
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --upgrade pip

##############
# FINAL stage
##############
FROM base AS final
COPY --from=build /app /app

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# create app user
RUN groupadd -g 1001 appuser && \
    useradd -u 1001 -g appuser -m -s /bin/bash appuser

RUN mkdir /home/appuser/.postgresql
ADD --chown=appuser:appuser https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /home/appuser/.postgresql/root.crt

RUN chown -R appuser:appuser /app/

USER 1001

EXPOSE 5000

CMD ["./docker-entrypoint.sh"]
