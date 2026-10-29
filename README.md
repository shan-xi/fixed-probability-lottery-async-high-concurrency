# Fixed Probability Lottery — Async High Concurrency

A high-concurrency lottery backend built with **Java 21, Spring Boot, PostgreSQL, Apache Kafka, and Docker Compose**.

The system supports fixed-probability prize selection, limited inventory for high-value prizes, asynchronous draw
processing, idempotent requests, and reliable database-to-message-broker event delivery through the Transactional Outbox
pattern.

> **Important:** This README describes the intended architecture and usage. Verify all commands, endpoint paths,
> configuration names, and response fields against the actual source code. A successful Docker image build and end-to-end
> integration test are required before considering the project deployment-ready.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Business Requirements](#2-business-requirements)
3. [Technology Stack](#3-technology-stack)
4. [System Architecture](#4-system-architecture)
5. [Lottery Probability Rules](#5-lottery-probability-rules)
6. [Limited Prize Inventory](#6-limited-prize-inventory)
7. [Asynchronous Request Flow](#7-asynchronous-request-flow)
8. [Transactional Outbox Pattern](#8-transactional-outbox-pattern)
9. [Idempotency and Duplicate Requests](#9-idempotency-and-duplicate-requests)
10. [Database Design](#10-database-design)
11. [Project Structure](#11-project-structure)
12. [Prerequisites](#12-prerequisites)
13. [Configuration](#13-configuration)
14. [Run the Project](#14-run-the-project)
15. [API Documentation and Examples](#15-api-documentation-and-examples)
16. [Testing](#16-testing)
17. [High-Concurrency Design](#17-high-concurrency-design)
18. [Failure Handling and Recovery](#18-failure-handling-and-recovery)
19. [Monitoring and Troubleshooting](#19-monitoring-and-troubleshooting)
20. [Production Considerations](#20-production-considerations)
21. [Future Improvements](#21-future-improvements)

---

## 1. Project Overview

Traditional synchronous lottery processing follows this sequence:

1. The client submits a draw request.
2. The API selects a prize.
3. The API writes the result to the database.
4. The API returns the result.

The client must wait for the processing to finish. Under heavy traffic, many API requests can be waiting for database
connections and transactions simultaneously.

This project uses an asynchronous processing model:

1. The API accepts and persists a draw request.
2. PostgreSQL stores the draw and an outbox event in the same transaction.
3. An outbox publisher sends the event to Kafka.
4. A lottery worker consumes the event.
5. The worker selects a prize and atomically enforces limited prize inventory.
6. The final result is persisted in PostgreSQL.
7. The client retrieves the result using the draw ID.

This design separates request acceptance from lottery execution and provides a foundation for buffering traffic spikes
and independently scaling API and worker instances.

### Main goals

- Support asynchronous lottery processing.
- Keep prize probabilities fixed.
- Limit the total number of high-value prizes awarded.
- Prevent duplicate requests from consuming multiple prizes.
- Avoid losing committed requests during the database-to-Kafka handoff.
- Support retry and recovery after temporary failures.
- Allow API and worker capacity to scale independently.

---

## 2. Business Requirements

### Prize probabilities

| Prize     | Probability | Inventory policy |
|-----------|------------:|------------------|
| Prize 1   |          1% | Limited          |
| Prize 2   |          2% | Limited          |
| Prize 3   |          5% | Unlimited        |
| Prize 4   |          7% | Unlimited        |
| Prize 5   |         10% | Unlimited        |
| Prize 6   |         12% | Unlimited        |
| Prize 7   |         13% | Unlimited        |
| Prize 8   |         15% | Unlimited        |
| Prize 9   |         15% | Unlimited        |
| Prize 10  |         20% | Unlimited        |
| **Total** |    **100%** |                  |

### User rules

- A user can have only one accepted lottery draw.
- Repeating a request for the same user must not create another draw.
- The final draw result must be persisted.
- Prize 1 and Prize 2 cannot exceed their configured quotas.
- If a limited prize is exhausted, the default business policy is to return `NO_PRIZE` when that prize is selected as
  the candidate.
- A draw request can initially remain `PENDING` while asynchronous processing is underway.

### Fixed probability versus guaranteed winning rate

The probabilities describe the candidate selected by the lottery algorithm.

For example, Prize 1 has a candidate-selection probability of 1%.

Because Prize 1 has limited inventory, the final probability of receiving Prize 1 can be lower once the quota is
exhausted.

The system does not redistribute exhausted prize probability to the remaining prizes under the default policy.

---

## 3. Technology Stack

| Technology     | Responsibility                                                        |
|----------------|-----------------------------------------------------------------------|
| Java 21        | Application runtime                                                   |
| Spring Boot    | REST API, dependency injection, configuration, transaction management |
| PostgreSQL     | Durable draw records, idempotency, inventory, outbox events           |
| Apache Kafka   | Asynchronous event transport and traffic buffering                    |
| Spring Kafka   | Kafka producer and consumer integration                               |
| Flyway         | Versioned database migrations                                         |
| Docker         | Container packaging                                                   |
| Docker Compose | Local multi-service environment                                       |
| k6             | HTTP load testing                                                     |

The exact versions should be confirmed in `pom.xml` and the Docker configuration.

---

## 4. System Architecture

```text
                    Client
                      |
                      | POST /api/lottery/draws
                      v
              +------------------+
              | Spring Boot API  |
              +------------------+
                      |
                      | PostgreSQL transaction
                      v
              +------------------+
              | PostgreSQL       |
              |                  |
              | draw records     |
              | outbox events    |
              | prize inventory  |
              +------------------+
                      |
                      | Outbox publisher
                      v
              +------------------+
              | Apache Kafka     |
              | Lottery events   |
              +------------------+
                      |
                      | Consume event
                      v
              +------------------+
              | Lottery Worker   |
              +------------------+
                      |
                      | Database transaction
                      v
              +------------------+
              | PostgreSQL       |
              |                  |
              | candidate prize  |
              | final result     |
              | inventory update |
              +------------------+
                      ^
                      |
                      | GET draw result
                      |
                    Client
```

### Why separate API and worker processing?

The API primarily accepts and persists requests. The worker performs the lottery business operation.

This separation allows:

- API instances to scale according to incoming HTTP traffic.
- Kafka to buffer requests when workers cannot keep up.
- Worker instances to scale according to processing backlog.
- Temporary worker failures to be recovered without requiring clients to resubmit new draws.

Kafka does not eliminate database bottlenecks. Database connection capacity, transaction duration, lock contention, and
worker concurrency must still be measured and tuned.

---

## 5. Lottery Probability Rules

The lottery selector uses a weighted random algorithm.

A convenient implementation uses 10,000 equally likely tickets.

The probability of each prize is converted into a ticket range:

| Prize     | Probability | Ticket count |
|-----------|------------:|-------------:|
| Prize 1   |          1% |          100 |
| Prize 2   |          2% |          200 |
| Prize 3   |          5% |          500 |
| Prize 4   |          7% |          700 |
| Prize 5   |         10% |        1,000 |
| Prize 6   |         12% |        1,200 |
| Prize 7   |         13% |        1,300 |
| Prize 8   |         15% |        1,500 |
| Prize 9   |         15% |        1,500 |
| Prize 10  |         20% |        2,000 |
| **Total** |    **100%** |   **10,000** |

The application generates a random integer in the range `[0, 10000)` and maps it to a prize.

Example:

```java
int ticket = ThreadLocalRandom.current().nextInt(10_000);
```

The selector uses the configured probability ranges to determine the candidate prize.

### Why use this approach?

The selector:

- Has constant-time selection complexity when implemented with cumulative ranges.
- Does not require database access to calculate a candidate.
- Does not require a global application lock.
- Does not need to update shared probability counters.
- Preserves the same candidate probability distribution across draws.

The candidate-selection algorithm is independent of inventory enforcement. Inventory must still be checked atomically by
the database.

---

## 6. Limited Prize Inventory

Prize 1 and Prize 2 have limited quotas.

Example defaults:

| Prize   | Default quota |
|---------|--------------:|
| Prize 1 |             1 |
| Prize 2 |            10 |

The quotas should be configurable using the project's supported application properties or environment variables.

For example:

```dotenv
PRIZE_1_QUOTA=1
PRIZE_2_QUOTA=10
```

Confirm that the application's configuration maps these variables to the actual Spring properties.

### Atomic quota enforcement

A worker must never perform the following as two independent operations:

1. Read `awarded_count`.
2. If the quota has not been reached, increment the count.

Two workers could read the same available count and both award the final prize.

Instead, use a conditional database update:

```sql
UPDATE lottery_prize_inventory
SET awarded_count = awarded_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE prize_type = :prize_type
  AND awarded_count < quota
RETURNING awarded_count;
```

The operation atomically checks the quota and increments the awarded count.

If the update returns a row, the quota was successfully consumed.

If it returns no row, the quota is exhausted.

The inventory update and final draw-result update must occur within the same database transaction.

### When the quota is exhausted

The default rule is:

```text
Selected candidate = PRIZE_1
Prize 1 quota exhausted
        |
        v
Final result = NO_PRIZE
```

The candidate prize and final result should be stored separately if the application needs to explain why the user did
not receive the candidate prize.

---

## 7. Asynchronous Request Flow

### Step 1: Client submits a request

```http
POST /api/lottery/draws
Content-Type: application/json
```

Example request:

```json
{
  "userId": 1001
}
```

The API validates the request and creates a draw record with a pending status.

### Step 2: Persist the request and outbox event

Within a single PostgreSQL transaction:

1. Insert the draw record.
2. Insert an outbox event describing the requested draw.
3. Commit the transaction.

The API should not report that the request has been durably accepted until this transaction commits.

If the transaction fails, neither record should be committed.

### Step 3: Return an acceptance response

A typical asynchronous API uses:

```http
202 Accepted
```

The response should include a stable draw ID and enough information for the client to retrieve the result.

Example:

```json
{
  "drawId": 1,
  "status": "PENDING"
}
```

This is an illustrative response. Use the actual response fields implemented by the application.

### Step 4: Publish the outbox event

The publisher reads unpublished outbox events and sends them to Kafka.

After Kafka acknowledges publication, the publisher marks the event as published.

If publication fails, the event remains eligible for retry.

### Step 5: Worker consumes the event

The lottery worker:

1. Reads the event.
2. Identifies the draw request.
3. Checks whether the draw has already been completed.
4. Selects a candidate prize if processing is still required.
5. Atomically consumes inventory when the candidate is limited.
6. Persists the final prize and completed status.
7. Commits the database transaction.
8. Acknowledges the Kafka message after successful processing.

### Step 6: Client retrieves the result

The client queries the draw status until processing completes.

The final result might be:

```json
{
  "drawId": 1,
  "status": "COMPLETED",
  "prizeType": "PRIZE_3"
}
```

The exact fields depend on the implementation.

---

## 8. Transactional Outbox Pattern

### The dual-write problem

Consider this unsafe sequence:

```text
1. Insert draw request into PostgreSQL
2. Commit PostgreSQL transaction
3. Publish event to Kafka
```

If the application crashes between steps 2 and 3, the database contains the request, but Kafka never receives the event.

Reversing the order does not solve the problem: Kafka might receive an event for a database transaction that
subsequently rolls back.

### Solution: Transactional Outbox

Persist the business record and the outbox event in the same database transaction.

```text
BEGIN;

INSERT INTO lottery_draws (...);

INSERT INTO outbox_events (...);

COMMIT;
```

A separate publisher delivers the outbox event to Kafka.

This removes the database/Kafka dual-write gap for the initial request.

### At-least-once publication

The outbox publisher may publish an event successfully and crash before marking it as published.

The event can then be published again.

Therefore, the system must tolerate duplicate delivery.

The outbox pattern does not guarantee that every event is published exactly once. Instead, it combines durable event
storage, retry, and idempotent consumers.

### Publisher concurrency

If multiple publisher instances run concurrently, the implementation must prevent unsafe competing claims. A typical
approach uses a short transaction with `FOR UPDATE SKIP LOCKED`, a claim/lease mechanism, or an equivalent reliable
ownership strategy.

Do not keep a database transaction open while waiting indefinitely for Kafka.

---

## 9. Idempotency and Duplicate Requests

Idempotency means that repeating the same logical request does not create additional business effects.

For this project, the intended rule is one draw per user.

A database constraint should enforce this rule, for example:

```sql
UNIQUE (user_id)
```

This is stronger than checking for an existing user in application code before inserting. Two concurrent requests can
both pass an application-level existence check.

### Duplicate HTTP requests

```text
Request A: userId = 1001
Request B: userId = 1001
          |
          v
Database unique constraint
          |
          v
Only one draw record is created
```

The API should return the existing draw for duplicate submissions rather than creating a new one.

### Duplicate Kafka events

The worker should treat the database as the source of truth.

If a draw is already completed, processing the same event again must not:

- Generate another final draw.
- Consume another limited prize quota.
- Change the already completed result.

The consumer should acknowledge the event only after its database work commits successfully.

A process crash after the database commit but before Kafka acknowledgement can cause redelivery. Idempotency handles
that situation.

---

## 10. Database Design

The logical database design contains three main areas.

### 10.1 Lottery draws

Stores the request and final outcome.

Possible fields:

| Field                 | Purpose                                             |
|-----------------------|-----------------------------------------------------|
| `id`                  | Unique draw identifier                              |
| `user_id`             | User identifier; unique for one-draw-per-user rules |
| `status`              | Processing state, such as `PENDING` or `COMPLETED`  |
| `selected_prize_type` | Candidate selected by the probability algorithm     |
| `prize_type`          | Final awarded result                                |
| `created_at`          | Request creation timestamp                          |
| `updated_at`          | Last update timestamp                               |

The exact schema may use different names or additional fields.

### 10.2 Prize inventory

Stores finite prize quotas.

| Field           | Purpose                    |
|-----------------|----------------------------|
| `prize_type`    | Inventory key              |
| `quota`         | Maximum number of awards   |
| `awarded_count` | Number of allocated prizes |
| `updated_at`    | Last update timestamp      |

Required invariant:

```text
0 <= awarded_count <= quota
```

Only limited prizes need finite inventory rows if that matches the implementation.

### 10.3 Outbox events

Stores events waiting for publication or requiring retry.

Typical fields include:

| Field           | Purpose                          |
|-----------------|----------------------------------|
| `id`            | Unique event identifier          |
| `aggregate_id`  | Associated draw identifier       |
| `event_type`    | Event type                       |
| `payload`       | Serialized event content         |
| `status`        | Publication status               |
| `attempt_count` | Number of publication attempts   |
| `created_at`    | Event creation timestamp         |
| `published_at`  | Successful publication timestamp |

The exact columns depend on the implemented outbox schema.

### Database migration strategy

Use Flyway migrations to version schema changes.

Do not rely on manually changing a local database without adding the corresponding migration.

For production deployments, review migrations before applying them to persistent data.

---

## 11. Project Structure

A typical project layout is:

```text
fixed-probability-lottery-async-high-concurrency/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/lottery/
│   │   │       ├── controller/
│   │   │       ├── service/
│   │   │       ├── repository/
│   │   │       ├── domain/
│   │   │       ├── kafka/
│   │   │       ├── outbox/
│   │   │       └── config/
│   │   └── resources/
│   │       ├── application.yml
│   │       └── db/migration/
│   └── test/
├── loadtest/
│   └── lottery-async.js
├── scripts/
├── Dockerfile
├── docker-compose.yml
├── pom.xml
├── .env.example
└── README.md
```

The package and file names above describe a suggested organization. Check the actual project tree for the exact
locations of each component.

### Main responsibilities

- **Controller:** accepts HTTP requests and returns API responses.
- **Service:** implements lottery business rules and transaction boundaries.
- **Repository:** performs database reads and writes.
- **Selector:** calculates the candidate prize.
- **Inventory service:** atomically enforces finite quotas.
- **Outbox publisher:** publishes durable events to Kafka.
- **Kafka consumer:** processes draw requests asynchronously.
- **Flyway:** manages database schema changes.

---

## 12. Prerequisites

Install the following:

- Java 21
- Docker Desktop
- Docker Compose
- Git, if cloning the repository
- k6, if running load tests

Check Java:

```bash
java -version
```

Check Docker:

```bash
docker --version
docker compose version
```

Check Maven if using a local Maven installation:

```bash
mvn -version
```

The project can also build inside Docker if its Dockerfile uses a Maven build stage. In that case, a local Maven
installation is not necessarily required.

---

## 13. Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Review the actual `.env.example` file before starting the application.

Example configuration:

```dotenv
DB_PASSWORD=change-me
PRIZE_1_QUOTA=1
PRIZE_2_QUOTA=10
```

These are example values, not a complete list of supported environment variables.

### Security

- Never commit production passwords or credentials.
- Use a secret manager or the deployment platform's secret mechanism in production.
- Do not expose PostgreSQL or Kafka publicly unless explicitly required and secured.
- Use authenticated and encrypted Kafka connections in production.

### Changing quotas

If the inventory initializer inserts rows only when they do not already exist, changing `PRIZE_1_QUOTA` or
`PRIZE_2_QUOTA` after the first database initialization will not necessarily change existing database values.

Inspect the initializer and migration before assuming environment variables automatically update persistent quotas.

Any quota adjustment should preserve:

```text
quota >= awarded_count
```

Do not lower a quota below the number of prizes already awarded.

---

## 14. Run the Project

From the project root:

### Step 1: Configure the environment

```bash
cp .env.example .env
```

Edit `.env` and set a suitable database password.

### Step 2: Start the services

```bash
docker compose up --build
```

This should build and start the services defined in the Compose file, typically including PostgreSQL, Kafka, and the
application.

### Step 3: Inspect logs

```bash
docker compose logs -f
```

If the Compose service is named `app`, inspect it specifically:

```bash
docker compose logs -f app
```

The service name must match the actual `docker-compose.yml`.

### Step 4: Check container status

```bash
docker compose ps
```

Wait until PostgreSQL, Kafka, and the application are healthy or ready according to their configured health checks.

A running container does not necessarily mean the application is ready to process requests.

### Step 5: Stop the environment

```bash
docker compose down
```

This stops and removes containers and the default Compose network.

Persistent database data may remain in a named volume.

To remove named volumes as well:

```bash
docker compose down -v
```

**Warning:** this can permanently delete local database data.

---

## 15. API Documentation and Examples

The examples below illustrate the intended API workflow. Confirm the actual endpoint paths and JSON fields against the
controller classes and DTOs in the source code.

### 15.1 Submit a draw

```bash
curl -i -X POST http://localhost:8080/api/lottery/draws \
  -H 'Content-Type: application/json' \
  -d '{"userId":1001}'
```

Expected behavior:

- A valid request is accepted.
- The database records the draw request.
- The outbox event is recorded transactionally.
- The API returns an acceptance response.
- The draw is processed asynchronously.

A conventional response is `202 Accepted` with a draw ID and pending status.

### 15.2 Retrieve a draw

If the implementation provides an ID-based endpoint, use the returned draw ID:

```bash
curl http://localhost:8080/api/lottery/draws/id/1
```

Poll until the draw reaches a terminal state.

Do not assume the example endpoint exists until you verify the controller mapping.

### 15.3 Retrieve prize definitions

```bash
curl http://localhost:8080/api/lottery/prizes
```

The response should describe the configured prize types and probabilities if this endpoint is implemented.

### 15.4 Retrieve prize inventory

```bash
curl http://localhost:8080/api/lottery/inventory
```

The response should expose the limited prize quotas and current usage if this endpoint is implemented.

### 15.5 Test idempotency

Submit the same user twice:

```bash
curl -i -X POST http://localhost:8080/api/lottery/draws \
  -H 'Content-Type: application/json' \
  -d '{"userId":1001}'
```

Repeat the request with the same user ID.

Verify that both responses refer to the same logical draw and that only one draw is persisted.

### 15.6 Test limited inventory

1. Configure a small Prize 1 quota.
2. Submit many requests from distinct users.
3. Wait for all accepted requests to finish processing.
4. Query the inventory endpoint or database.
5. Confirm that the awarded count never exceeds the quota.

A probabilistic selector alone is not sufficient to enforce a quota.

---

## 16. Testing

Use multiple levels of testing. A successful unit test does not establish that Kafka, PostgreSQL, Docker, and the HTTP
API work correctly together.

### 16.1 Unit tests

Test:

- The sum of prize probabilities is 100%.
- Ticket boundaries map to the correct candidate prizes.
- Candidate selection never returns an invalid prize.
- `NO_PRIZE` is not included in the weighted candidate ranges.
- Probability configuration is validated.

Run:

```bash
mvn test
```

This command should be run from the project root.

### 16.2 Integration tests

Integration tests should use a real PostgreSQL instance and, where appropriate, a real Kafka broker.

Test:

- A draw request is persisted.
- A duplicate user does not create another draw.
- An outbox event is created in the same transaction as the draw.
- The publisher retries failed publication.
- The consumer completes the draw.
- Duplicate Kafka delivery does not consume inventory twice.
- The inventory limit is enforced under concurrent processing.
- A worker crash can be recovered through redelivery.
- A completed draw remains unchanged after duplicate processing.

If tests use Testcontainers, Docker must be available.

### 16.3 Load testing

Use k6 to test HTTP request acceptance and, separately, end-to-end result completion.

An HTTP acceptance test alone measures API throughput; it does not establish how quickly the lottery worker completes
requests.

Measure at least:

- HTTP requests per second.
- HTTP error rate.
- p50, p95, and p99 API latency.
- Number of accepted requests.
- Number of completed draws.
- Kafka consumer lag.
- Outbox backlog and oldest unpublished event age.
- PostgreSQL connection-pool saturation.
- Database transaction latency.
- Inventory lock contention.
- End-to-end completion latency.

### 16.4 Example k6 command

If the project includes `loadtest/lottery-async.js`:

```bash
k6 run loadtest/lottery-async.js
```

Check the script's environment variables and request format before running it.

For a valid end-to-end test, the test should also track draw IDs and verify that accepted draws eventually reach a
terminal status.

---

## 17. High-Concurrency Design

### 17.1 Kafka buffers bursts

When requests arrive faster than workers can process them, Kafka can temporarily retain events.

```text
Incoming traffic > worker throughput
                |
                v
        Kafka backlog increases
                |
                v
        Workers catch up later
```

Kafka provides buffering, not unlimited capacity. If the sustained arrival rate exceeds processing capacity, the backlog
continues to grow.

### 17.2 Partition count and consumer concurrency

Kafka partitions allow parallel consumption within a consumer group.

For example, a topic with 12 partitions can assign work to up to 12 active consumers in that group at once, subject to
the actual deployment and consumer configuration.

Increasing the number of consumers beyond the number of partitions does not increase active parallelism for that topic.

Partition count should be chosen based on expected throughput, ordering requirements, and operational constraints.

### 17.3 Database connection pools

Every API instance and worker instance may use its own database connection pool.

The aggregate maximum connection demand is approximately:

```text
API instances × API pool maximum
+
worker instances × worker pool maximum
+
other database clients
```

This must fit within the PostgreSQL connection budget.

Increasing the worker count without controlling connection pools can overload PostgreSQL and reduce throughput.

### 17.4 Atomic inventory updates

Finite inventory is shared mutable state.

The database must enforce the quota under concurrent transactions. Do not rely on a Java synchronized block or an
in-memory counter: those protections do not span multiple application instances.

### 17.5 Avoid unnecessary global locks

The probability selector should not require a global lock.

Use database atomic operations for limited inventory and unique constraints for one-draw-per-user rules.

This lets independent draws proceed concurrently, except where they compete for the same limited inventory row.

### 17.6 Backpressure

A robust system should monitor queue depth and consumer lag and define a policy for excessive backlog.

Possible strategies include:

- Scaling consumers.
- Applying admission control.
- Rate-limiting incoming traffic.
- Rejecting requests before durable acceptance when the service is overloaded.
- Alerting when backlog age exceeds a threshold.

Never silently discard accepted draw requests to control load.

---

## 18. Failure Handling and Recovery

| Failure                                                | Expected handling                                                                       |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------|
| PostgreSQL unavailable                                 | API cannot durably accept new requests; return an appropriate error                     |
| Kafka unavailable                                      | Keep committed outbox events for retry                                                  |
| Publisher crashes                                      | Resume publishing eligible outbox events                                                |
| Event delivered twice                                  | Consumer idempotency prevents duplicate business effects                                |
| Worker crashes before commit                           | Transaction rolls back; message can be retried                                          |
| Worker crashes after commit but before acknowledgement | Message may be redelivered; completed draw remains unchanged                            |
| Prize quota exhausted                                  | Apply the configured exhausted-prize policy                                             |
| Poison event repeatedly fails                          | Retry with limits and move to a dead-letter workflow or other explicit recovery process |
| Consumer lag grows                                     | Scale or tune workers and investigate database bottlenecks                              |

### Retry policy

Retries should have:

- A bounded or explicitly managed retry policy.
- Exponential backoff where appropriate.
- Error classification.
- Metrics and logs.
- A recovery procedure for dead-lettered events.

A failed message should not be silently acknowledged and lost.

### Event ordering

If the system needs ordering for the same user or draw, choose the Kafka message key and partitioning strategy
accordingly.

Kafka only preserves ordering within a partition. It does not guarantee global ordering across all partitions.

---

## 19. Monitoring and Troubleshooting

### Docker build errors

If Docker cannot find the JAR file during the image build, check the artifact name in `pom.xml`.

For example:

```xml

<artifactId>fixed-probability-lottery-async-high-concurrency</artifactId>
<version>1.0.0</version>
```

This normally produces:

```text
target/fixed-probability-lottery-async-high-concurrency-1.0.0.jar
```

The `COPY --from=build` instruction in the Dockerfile must match the actual JAR path and name.

### Application startup errors

Inspect:

```bash
docker compose logs -f app
```

Check:

- Database credentials.
- PostgreSQL readiness.
- Kafka bootstrap server configuration.
- Flyway migration errors.
- Spring property names.
- Port conflicts.
- Java and Maven build errors.

### Kafka consumer lag

High consumer lag may indicate:

- Too few consumers.
- Slow database transactions.
- Excessive inventory contention.
- Long-running business logic.
- Frequent retries.
- A database connection pool bottleneck.

Do not increase consumer concurrency without measuring database impact.

### Outbox backlog

If outbox events remain unpublished:

1. Inspect publisher logs.
2. Check Kafka connectivity and authentication.
3. Check event status and retry count.
4. Check whether events are stuck in an invalid state.
5. Verify that the publisher's claim and retry mechanism is operating correctly.

### Inventory mismatch

Compare the inventory count against persisted completed draws for each limited prize.

The invariant is:

```text
awarded_count <= quota
```

If the count represents successful quota allocations, reconcile it against the authoritative draw records and the exact
business rules for inventory accounting.

Do not manually edit counts without understanding the affected records and transaction history.

---

## 20. Production Considerations

This project is a foundation for learning asynchronous high-concurrency design. A local Docker Compose setup is not, by
itself, production-ready.

Before deployment, address the following.

### Reliability

- Use durable Kafka topics and an appropriate replication factor.
- Configure acknowledgements and producer reliability settings.
- Use appropriate consumer acknowledgement semantics.
- Implement safe outbox claiming, retries, and recovery.
- Ensure consumer operations are idempotent.
- Test crash recovery at transaction boundaries.
- Configure dead-letter handling.

### Security

- Authenticate and authorize administrative endpoints.
- Use TLS and suitable credentials for Kafka and database connections.
- Keep secrets out of source control.
- Validate incoming requests.
- Apply rate limiting and abuse protection.

### Observability

- Add metrics for API latency, error rate, worker throughput, and queue lag.
- Track outbox age and retry counts.
- Add structured logs and correlation IDs.
- Use distributed tracing if the deployment spans multiple services.
- Configure alerts for stalled processing and quota anomalies.

### Database scalability

- Tune connection pools.
- Index common query paths.
- Keep transactions short.
- Review lock contention on inventory rows.
- Plan backups, point-in-time recovery, and database failover.
- Test schema migrations against production-sized data.

### Scaling

- Scale API instances independently from worker instances.
- Monitor Kafka partitions and consumer assignments.
- Avoid overloading PostgreSQL by scaling workers too aggressively.
- Define load shedding and admission-control policies.

### Business semantics

Before launch, define:

- Whether the candidate prize or final awarded prize is displayed to users.
- Whether exhausted prizes become `NO_PRIZE` or trigger a redraw.
- Whether the one-draw-per-user rule is global or campaign-specific.
- Whether a draw request can be cancelled.
- How long request and event records are retained.
- Whether quotas can change after a campaign starts.
- How administrative quota changes are audited.

---

## 21. Future Improvements

Potential improvements include:

1. Add an explicit API idempotency key for clients that need request-level deduplication beyond the user ID constraint.
2. Add a durable campaign identifier if multiple campaigns share the same user population.
3. Implement a dead-letter queue and an administrative replay workflow.
4. Add metrics dashboards for Kafka lag, outbox backlog, and draw completion latency.
5. Add a proper end-to-end k6 test that submits requests, tracks draw IDs, and measures completion latency.
6. Add a benchmark profile to compare synchronous and asynchronous implementations.
7. Add integration tests for application crashes and redelivery.
8. Add authentication and authorization.
9. Add a production Kafka deployment with replication and monitoring.
10. Add a reconciliation job that verifies inventory counters against final draw records.
11. Add database partitioning or retention policies if draw volume becomes large.
12. Add a campaign configuration model to support multiple probability maps and independent inventory quotas.

---

## Final Summary

The main design principles are:

- **Kafka** buffers requests and decouples API acceptance from processing.
- **Transactional Outbox** prevents the database-to-Kafka dual-write gap for accepted requests.
- **PostgreSQL unique constraints** enforce one draw per user.
- **Atomic SQL inventory updates** prevent limited prizes from being over-awarded.
- **Idempotent consumers** make duplicate message delivery safe.
- **Independent API and worker scaling** allows capacity to be adjusted according to actual bottlenecks.
- **Metrics and end-to-end load tests** establish whether the complete system meets its throughput and latency targets.

The goal is not to claim unlimited concurrency. It is to design a system that can absorb bursts, preserve business
invariants, recover from failures, and scale predictably based on measured capacity.
