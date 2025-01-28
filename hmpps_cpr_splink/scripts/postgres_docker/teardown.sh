#!/bin/bash

# remove -v (removing volume) if you want to keep data
docker compose -f docker-compose.yaml down -v
