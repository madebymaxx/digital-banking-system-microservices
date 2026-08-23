# Digital Banking System — Microservices

A digital banking backend built using a microservices architecture. Services communicate asynchronously through Apache Kafka, fraud detection runs in real time using Redis, and payments are handled through Razorpay.

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-black)
![Redis](https://img.shields.io/badge/Redis-Fraud%20Detection-red)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Services](#services)
- [System Architecture](#system-architecture)
- [How a Transaction Flows Through the System](#how-a-transaction-flows-through-the-system)
- [Kafka Topics](#kafka-topics)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Author](#author)

---

## Overview

This project is a simulation of a real-world digital banking platform, broken down into independently deployable microservices. Instead of one service calling another directly over REST, most of the internal communication happens through Kafka events — so services stay decoupled and can fail or scale independently without taking the whole system down.

A few things this project tries to get right:

- Each service owns exactly one responsibility
- Services talk to each other through events, not tight synchronous calls
- Fraud checks happen in real time, in the actual transaction path, not after the fact
- All external traffic passes through a single gateway that handles rate limiting

---

## Features

- **API Gateway** — single entry point for all client requests, with rate limiting
- **Account Management** — create accounts and track balances
- **Transaction Processing** — transfers between accounts, with full history
- **Payments** — Razorpay integration with webhook handling
- **Fraud Detection** — real-time checks using Redis-backed pattern matching
- **Notifications** — alerts for transactions and fraud events
- **Event-driven core** — Kafka connects every service without direct dependencies between them

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| `api-gateway` | 8080 | Single entry point, request routing, rate limiting |
| `account-service` | 8081 | Account creation, management, and balance tracking |
| `transaction-service` | 8082 | Money transfers, transaction history |
| `payment-service` | 8083 | Razorpay integration, payment webhooks |
| `fraud-detection-service` | 8084 | Real-time fraud detection using Redis |
| `notification-service` | 8085 | Transaction and fraud alerts (email/SMS) |

---

## System Architecture

```
                              ┌──────────────┐
                              │    Client     │
                              └───────┬───────┘
                                      ▼
                              ┌──────────────┐
                              │ API Gateway   │
                              │ (Rate Limit)  │
                              └───────┬───────┘
                                      ▼
                 ┌────────────────────────────────────┐
                 │  Account · Transaction · Payment     │
                 │              Services                │
                 └────────────────────┬─────────────────┘
                                      ▼
                              ┌──────────────┐
                              │ Apache Kafka  │
                              └───────┬───────┘
                     ┌────────────────┴────────────────┐
                     ▼                                   ▼
          ┌──────────────────────┐          ┌───────────────────────┐
          │  Fraud Detection      │          │  Notification Service │
          │  Service (Redis)      │          │  (Email / SMS Alerts) │
          └───────────┬────────────┘          └───────────────────────┘
                     ▼
          ┌──────────────────────┐
          │  Account Service      │
          │  (Block if fraud)     │
          └──────────────────────┘
```

---

## How a Transaction Flows Through the System

1. A client sends a request through the API Gateway, which checks rate limits and routes it to the right service.
2. The Transaction Service starts a transfer and publishes a `transaction.initiated` event to Kafka.
3. The Fraud Detection Service picks up this event, runs pattern checks against Redis, and publishes the result as `fraud.check.result`.
4. If the transaction looks clean, the Transaction Service completes it and publishes `transaction.completed`.
5. If fraud is flagged instead, a `fraud.detected` event goes out — the Account Service picks this up and blocks the account.
6. The Notification Service listens for both completed transactions and fraud events, and sends the right alert to the user.
7. Payments made through Razorpay go through the Payment Service, which publishes `payment.completed` once the webhook confirms the payment.

---

## Kafka Topics

| Topic | Publisher | Consumer(s) |
|---|---|---|
| `transaction.initiated` | Transaction Service | Fraud Detection Service |
| `fraud.check.result` | Fraud Detection Service | Transaction Service |
| `transaction.completed` | Transaction Service | Account Service, Notification Service |
| `fraud.detected` | Fraud Detection Service | Account Service, Notification Service |
| `payment.completed` | Payment Service | Notification Service |

---

## Tech Stack

- **Language / Framework:** Java 21, Spring Boot 3.x, Spring Security
- **Messaging:** Apache Kafka
- **Caching / Real-time detection:** Redis
- **Payments:** Razorpay API
- **Containerization:** Docker, Docker Compose
- **Build tool:** Maven

---

## Prerequisites

- Java 21+
- Maven 3.8+
- Docker & Docker Compose
- Razorpay API keys (test mode works fine for local dev)
- Postman (optional, for testing endpoints)

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/madebymaxx/Digital-Banking-System-Microservices.git
cd Digital-Banking-System-Microservices
```

### 2. Start infrastructure

```bash
docker-compose up -d
```

This brings up Kafka, Redis, and everything else the services depend on. Give it a minute, then check everything's actually running:

```bash
docker-compose ps
```

### 3. Start each service

Each service runs independently — open a separate terminal for each one:

```bash
# Terminal 1
cd account-service && mvn spring-boot:run

# Terminal 2
cd transaction-service && mvn spring-boot:run

# Terminal 3
cd payment-service && mvn spring-boot:run

# Terminal 4
cd fraud-detection-service && mvn spring-boot:run

# Terminal 5
cd notification-service && mvn spring-boot:run

# Terminal 6 (start this one last)
cd api-gateway && mvn spring-boot:run
```

Start the API Gateway last, once everything else is already running.

### 4. Access the system

Once everything's up, all requests go through:

```
http://localhost:8080
```

---

## Project Structure

```
Digital-Banking-System-Microservices/
├── account-service/
├── api-gateway/
├── fraud-detection-service/
├── notification-service/
├── payment-service/
├── transaction-service/
├── docker-compose.yml
└── README.md
```

Each service follows a standard Spring Boot layout (`controller`, `service`, `repository`, `model`, `config`) and can be built and run on its own.

---

## Configuration

Each service reads its config from its own `application.yml` / `application.properties`. At minimum you'll usually need:

- Database connection details, if the service uses one
- Kafka broker address (`localhost:9092` by default with the included Docker Compose setup)
- Redis host/port for the fraud detection service
- Razorpay `key_id` and `key_secret` for the payment service

Keep any real credentials out of version control — use environment variables or a local, git-ignored config file instead.

---

## Troubleshooting

| Issue | Likely cause | Fix |
|---|---|---|
| Services can't connect to Kafka | Infrastructure isn't fully up yet | Run `docker-compose ps` and wait for Kafka to report healthy |
| Fraud detection isn't triggering | Redis isn't reachable | Check the Redis container is running and the port matches your config |
| Payment webhook not firing | Razorpay keys missing or wrong | Double-check `key_id` / `key_secret` in the payment service config |
| Gateway returns 404 or 502 | A downstream service isn't running | Make sure every service is up before hitting the gateway |

---

## Contributing

Contributions are welcome. Open an issue first if you want to discuss a change, then send a pull request.

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push the branch (`git push origin feature/your-feature`)
5. Open a pull request

---

## Author
GitHub: [github.com/madebymaxx](https://github.com/madebymaxx)

If this project was useful to you, consider giving it a star.
