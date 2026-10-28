# syntax=docker/dockerfile:1
FROM ghcr.io/ministryofjustice/hmpps-python:python3.13-alpine AS base
USER root

# load in build details
ARG BUILD_NUMBER
ARG GIT_REF
ARG GIT_BRANCH

ENV TZ=Europe/London

ENV APP_BUILD_NUMBER=${BUILD_NUMBER} \
    APP_GIT_REF=${GIT_REF} \
    APP_GIT_BRANCH=${GIT_BRANCH}

# Update pip
RUN python -m pip install --upgrade pip

# prevents python creating .pyc files
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_LOCK_TIMEOUT=600

##############
# BUILD stage
##############
FROM base AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.4 /uv /bin/uv

RUN apk add --no-cache build-base

ENV CMAKE_BUILD_PARALLEL_LEVEL=1

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
RUN apk add --no-cache bash libstdc++ && \
    mkdir -p /home/appuser/.postgresql && \
    chown -R appuser:appgroup /app /home/appuser

ADD --chown=appuser:appgroup https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /home/appuser/.postgresql/root.crt

USER appuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/5000 && printf "GET /health HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n" >&3 && grep -q "200 OK" <&3'

CMD ["/bin/sh", "/app/docker-entrypoint.sh"]
