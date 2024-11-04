# Database Services Stack

A Docker Compose configuration for running PostgreSQL (TimescaleDB), MongoDB, and Redis services. This implementation for using for local development. Do necessary modifications for other use cases.

## Prerequisites

- Docker
- Docker Compose

## Services

- **TimescaleDB** (PostgreSQL 15)
  - Port: 5432
  - Credentials: postgres/postgres
  - Database: timeseries_db

- **MongoDB**
  - Port: 27017
  - Credentials: mongo/mongo

- **Redis**
  - Port: 6379

## Usage

Start the services:
```bash
docker compose up -d
```

Stop the services:
```bash
docker compose down
```

All data is persisted in Docker volumes: `postgres_data`, `mongodb_data`, and `redis_data`.
