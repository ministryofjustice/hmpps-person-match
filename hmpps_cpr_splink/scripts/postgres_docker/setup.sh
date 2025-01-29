#!/bin/bash

# add -d for detached mode (run in background)
docker compose -f docker-compose.yaml up --build
