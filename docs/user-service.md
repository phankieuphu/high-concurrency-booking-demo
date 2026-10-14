# User Service (pre-existing base service)

> Moved from the root README. This Go service (`cmd/`, `internal/`, `pkg/` at the repo root) issues the HS256 JWTs (`user_id` claim) that the booking gateway verifies.

## Overview

Base code to create new another repository

---

## Repository Purpose

* Clean and normalize data from multiple sources
* Prepare data for banking reports
* Support extensible data ingestion (DB, Queue, etc.)
* Follow layered / hexagonal architecture

---

## Setup Guide

### Local Environment

1. Create environment variables:

```bash
cp .env.example .env
```

2. Update your local configuration in `.env`

3. Run the initialization script:

```bash
sh init.sh
``` 

---

### Docker Setup

```bash
docker compose up -d
```

---

## Initializing a New Data Flow

### 1. Define Data Sources

#### From Database

* Implement repository adapters

#### From Queue

* Location: `internal/adapters/consumer`
* Steps:

   * Add a new consumer: `{name}Consumer.go`
   * Define input DTOs in the `/dto` folder

---

### 2. Define a New Service

1. Define service interface:

   * File: `internal/domain/ports/services.go`

2. Implement service logic:

   * Folder: `internal/domain/services`

3. Inputs & outputs:

   * Use DTOs from `internal/adapters/http` if the service is HTTP-based

---

### 3. Define Outbound Adapters (Repositories)

For database or external storage operations:

1. Define repository interface:

   * `internal/domain/ports/repositories.go`

2. Create adapter struct:

   * `internal/adapters/repositories`

3. Implement repository logic

---

## Service Architecture Layers

```
Config
  |
DB Provider
  |
Repository (Storage)
  |
Service (Use Case)
```



---



## Database Configuration

* Define database models in:

```
internal/adapters/database/models
```
---
* **Note**: if your table want to define is SQL please update file **init.sql** your SQL script

## Testing

* Write unit tests for services and repositories
* Mock external dependencies
* Run tests using standard Go **tooling**
* Run `golangci-lint run` for ensure correct syntax
---

## Deployment

* Docker-based deployment
* Environment-driven configuration
* CI/CD friendly

---

## Contribution Guidelines

* Write tests for all new features
* Follow existing code structure
* Code reviews are mandatory

---

## Contact

* Repository owner / admin
* Project team members
