# syntax=docker/dockerfile:1
FROM python:3.12.7-slim-bullseye

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
    # paths
    # this is where our requirements + virtual environment will live
    VENV_PATH=".venv"


RUN apt-get install --no-install-recommends -y \
        # deps for installing uv
        curl \
        ca-certificates \
        # deps for building python deps
        build-essential

ADD https://astral.sh/uv/0.5.24/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

# copy project requirement files here to ensure they will be cached.
COPY uv.lock pyproject.toml ./

COPY ./hmpps_person_match /app/hmpps_person_match/
COPY docker-entrypoint.sh asgi.py alembic.ini /app/

# install Python dependencies in virtual environment
RUN uv sync --frozen --no-dev

WORKDIR /app/

# create app user
RUN groupadd -g 1001 appuser && \
    useradd -u 1001 -g appuser -m -s /bin/bash appuser

RUN mkdir /home/appuser/.postgresql
ADD --chown=appuser:appuser https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /home/appuser/.postgresql/root.crt

RUN chown appuser:appuser /app/
USER 1001

EXPOSE 5000

CMD ["./docker-entrypoint.sh"]
