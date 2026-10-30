# Fixed-Probability Lottery — Asynchronous High-Concurrency Demo

A Java 21 / Spring Boot 3.5 application demonstrating an asynchronous lottery draw pipeline using PostgreSQL, a
transactional outbox, Apache Kafka, Prometheus, Grafana, and Docker Compose.

> **Purpose:** learning, local development, and interview/system-design demonstrations. The included Kafka and
> PostgreSQL deployments are single-node development configurations, not production high-availability infrastructure.

## 1. Technology stack

| Component             | Version / role                |
|-----------------------|-------------------------------|
| Java                  | 21                            |
| Spring Boot           | 3.5.6                         |
| PostgreSQL            | 16                            |
| Apache Kafka          | 3.9.1, KRaft single node      |
| Flyway                | Database migrations           |
| HikariCP              | JDBC connection pool          |
| Micrometer + Actuator | Application metrics           |
| Prometheus            | Metrics collection            |
| Grafana               | Metrics dashboards            |
| postgres_exporter     | PostgreSQL metrics            |
| k6                    | HTTP load-test starter script |

## 2. Lottery business rules

The weighted prize probabilities are fixed and total 100%.

| Prize     | Probability | Default quota |
|-----------|------------:|--------------:|
| PRIZE_1   |          1% |             1 |
| PRIZE_2   |          2% |            10 |
| PRIZE_3   |          5% |     Unlimited |
| PRIZE_4   |          7% |     Unlimited |
| PRIZE_5   |         10% |     Unlimited |
| PRIZE_6   |         12% |     Unlimited |
| PRIZE_7   |         13% |     Unlimited |
| PRIZE_8   |         15% |     Unlimited |
| PRIZE_9   |         15% |     Unlimited |
| PRIZE_10  |         20% |     Unlimited |
| **Total** |    **100%** |               |

When a limited prize is selected after its quota has been exhausted, this demo converts the final result to `NO_PRIZE`.
It does not redistribute the probability to other prizes. This policy must be clearly disclosed in real campaign terms.

## 3. Architecture and request flow

```text
Client
  |
  | POST /api/lottery/draws
  v
Spring Boot API
  |
  | One PostgreSQL transaction
  | 1. Insert PENDING draw
  | 2. Insert outbox event
  v
PostgreSQL
  |
  | Outbox publisher polls and claims rows
  | FOR UPDATE SKIP LOCKED + claim lease
  v
Kafka topic: lottery.draw.requested
  |
  | Consumer group: lottery-draw-workers
  v
LotteryDrawWorker
  |
  | One PostgreSQL transaction
  | 1. Lock draw row / check PENDING
  | 2. Select weighted prize
  | 3. Atomically claim limited inventory
  | 4. Save final result
  v
PostgreSQL
  |
  v
Client polls draw status
```

### 3.1 API submission transaction

`POST /api/lottery/draws` creates a `PENDING` draw and its outbox event in the **same PostgreSQL transaction**. The API
returns `202 Accepted`; it does not wait for Kafka or the final draw result.

This prevents the dual-write failure where the draw commits but the process crashes before publishing the Kafka event.

### 3.2 Outbox claim and publish

`OutboxBatchClaimer` claims a batch using `FOR UPDATE SKIP LOCKED`, assigns a `claim_token` and `locked_until`, and
commits its short database transaction.

`OutboxBatchPublisher` performs Kafka network I/O **after** the claim transaction has completed. It marks an event as
published only when the Kafka send receives acknowledgement. A failed send releases the current claim for retry. If the
process crashes, the lease eventually expires and the row becomes eligible for another claim.

There is an unavoidable failure window: Kafka may accept an event, then the publisher may crash before PostgreSQL
records `published_at`. The event can be delivered again. Therefore delivery is **at least once**, not exactly once.

### 3.3 Idempotent consumer

The Kafka listener delegates to the worker. The worker checks/locks the draw row and processes only a draw still in
`PENDING`. A duplicate event for a completed draw does not award another prize.

### 3.4 Limited prize inventory

The quota claim is an atomic conditional update:

```sql
UPDATE lottery_prize_inventory
SET awarded_count = awarded_count + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE prize_type = ?
  AND awarded_count < quota
RETURNING awarded_count;
```

The quota claim and final draw update run in the same transaction. Concurrent requests cannot exceed the configured
quota. A scarce prize inventory row can become a hot row; that contention is the cost of enforcing a strict finite
quota.

## 4. Repository layout

```text
.
├── src/main/java/com/example/lottery
│   ├── api/                 # REST controllers and response models
│   ├── config/              # Application configuration
│   ├── domain/              # Prize types and weighted selector
│   └── service/             # Draw, worker, inventory, outbox and Kafka flow
├── src/main/resources
│   ├── application.yml      # Spring, datasource, Kafka and metrics config
│   └── db/migration/        # Flyway SQL migrations
├── src/test/                # Unit and integration tests
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/provisioning/
│       ├── datasources/
│       └── dashboards/
├── loadtest/                # k6 HTTP submission starter
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

## 5. Prerequisites

- Docker Desktop with Docker Compose v2
- Java 21 and Maven only if running/building the application outside Docker
- k6 only if running the load test

Check versions:

```bash
docker --version
docker compose version
java -version
mvn -version
```

## 6. Configure environment

The repository contains `.env.example`. Because it is a dotfile, use `ls -la` to see it.

```bash
cp .env.example .env
```

Edit `.env` and change at least:

```dotenv
DB_PASSWORD=your-long-random-postgres-password
GRAFANA_USER=admin
GRAFANA_PASSWORD=your-long-random-grafana-password
```

Docker Compose automatically reads `.env` from the project directory. Do not commit `.env`; commit `.env.example`
instead.

### Environment variables

| Variable                     | Default in example | Meaning                                     |
|------------------------------|-------------------:|---------------------------------------------|
| `DB_PASSWORD`                |          change-me | PostgreSQL and app DB password              |
| `DB_POOL_SIZE`               |                 20 | Hikari maximum connections per app instance |
| `DB_POOL_MIN_IDLE`           |                  5 | Hikari minimum idle connections             |
| `DB_CONNECTION_TIMEOUT_MS`   |               3000 | Wait limit for a connection from Hikari     |
| `DB_IDLE_TIMEOUT_MS`         |             600000 | Hikari idle connection timeout              |
| `DB_MAX_LIFETIME_MS`         |            1800000 | Hikari maximum connection lifetime          |
| `PRIZE_1_QUOTA`              |                  1 | PRIZE_1 inventory quota                     |
| `PRIZE_2_QUOTA`              |                 10 | PRIZE_2 inventory quota                     |
| `OUTBOX_POLL_DELAY_MS`       |                250 | Delay between outbox polling runs           |
| `OUTBOX_BATCH_SIZE`          |                100 | Maximum rows claimed per poll               |
| `OUTBOX_LEASE_SECONDS`       |                 60 | Lease duration for claimed outbox events    |
| `KAFKA_CONSUMER_CONCURRENCY` |                  3 | Listener concurrency per app instance       |
| `GRAFANA_USER`               |              admin | Grafana local login                         |
| `GRAFANA_PASSWORD`           |          change-me | Grafana local password                      |

Pool size is **per application instance**. For example, three application instances with `DB_POOL_SIZE=20` can open up
to 60 application connections. Keep the combined total below PostgreSQL's connection capacity and reserve connections
for administration and other services.

## 7. Start the complete stack

From the project root:

```bash
docker compose up --build -d
```

Check containers:

```bash
docker compose ps
```

Follow application logs:

```bash
docker compose logs -f app
```

Follow all services:

```bash
docker compose logs -f
```

Stop containers without deleting persisted database/Grafana data:

```bash
docker compose down
```

Reset the local demo completely, including PostgreSQL and Grafana volumes:

```bash
docker compose down -v
```

> `down -v` permanently deletes the local demo database and Grafana state.

## 8. Service URLs

| Service            | URL / port                                  | Purpose                     |
|--------------------|---------------------------------------------|-----------------------------|
| Lottery API        | `http://localhost:8080`                     | REST API                    |
| Actuator health    | `http://localhost:8080/actuator/health`     | App health                  |
| Prometheus metrics | `http://localhost:8080/actuator/prometheus` | Raw app metrics             |
| Prometheus UI      | `http://localhost:9090`                     | Query metrics               |
| Grafana            | `http://localhost:3000`                     | Dashboards                  |
| PostgreSQL         | `localhost:5433`                            | Database                    |
| Kafka              | `localhost:9092`                            | Local Kafka bootstrap       |
| postgres_exporter  | `http://localhost:9187/metrics`             | PostgreSQL exporter metrics |

Grafana login is the `GRAFANA_USER` and `GRAFANA_PASSWORD` from `.env`.

## 9. API test flow

### 9.1 Submit a draw

```bash
curl -i -X POST http://localhost:8080/api/lottery/draws \
  -H 'Content-Type: application/json' \
  -d '{"userId":1001}'
```

A first submission should return `202 Accepted` with a draw ID and `PENDING` status. Submitting again for the same user
returns the existing draw rather than creating another draw.

### 9.2 Poll by draw ID

Replace `1` with the `drawId` returned by the POST:

```bash
curl http://localhost:8080/api/lottery/draws/id/1
```

### 9.3 Poll by user ID

```bash
curl http://localhost:8080/api/lottery/draws/1001
```

Poll until the status is `COMPLETED`.

### 9.4 Inspect prizes and remaining inventory

```bash
curl http://localhost:8080/api/lottery/prizes
curl http://localhost:8080/api/lottery/inventory
```

### API summary

| Method | Endpoint                         | Description                   |
|--------|----------------------------------|-------------------------------|
| POST   | `/api/lottery/draws`             | Submit one draw per user      |
| GET    | `/api/lottery/draws/id/{drawId}` | Get draw by draw ID           |
| GET    | `/api/lottery/draws/{userId}`    | Get draw by user ID           |
| GET    | `/api/lottery/prizes`            | Get fixed prize probabilities |
| GET    | `/api/lottery/inventory`         | Get limited prize inventory   |

This is a learning demo. The request body accepts `userId` directly. A production API should derive the user identity
from an authenticated principal, not trust a client-supplied user ID.

## 10. Run tests

With Java 21 and Maven installed:

```bash
mvn test
```

The integration tests use Testcontainers and may require Docker to be running.

For a container image build:

```bash
docker compose build --no-cache app
```

## 11. k6 load-test starter

The supplied `loadtest/lottery-async.js` exercises the asynchronous **submission endpoint** and checks for `202` or
idempotent `200` responses. It does **not** poll each draw to completion, so it is not a full end-to-end
completion-latency test.

Install k6, then from the project root:

```bash
k6 run \
  -e BASE_URL=http://localhost:8080 \
  -e RATE=100 \
  -e DURATION=30s \
  -e VUS=100 \
  loadtest/lottery-async.js
```

Use unique user IDs for each test run. Watch API latency, Hikari pool pressure, PostgreSQL activity, Kafka consumer
progress, and application errors. Increase load gradually; more virtual users do not automatically mean higher
throughput.

For a proper async end-to-end benchmark, extend the script to capture each returned `drawId`, poll until `COMPLETED` or
timeout, and report submission latency separately from completion latency.

## 12. Grafana: open and build a dashboard

### 12.1 Open Grafana

1. Start the stack: `docker compose up --build -d`.
2. Open `http://localhost:3000`.
3. Sign in using `GRAFANA_USER` and `GRAFANA_PASSWORD` from `.env`.
4. Open **Dashboards** in the left navigation.
5. The project provisions a Prometheus data source and a starter **Lottery Overview** dashboard automatically. If it
   does not appear immediately, refresh the page after the containers finish starting.

The dashboard is provisioned from:

```text
monitoring/grafana/provisioning/datasources/prometheus.yml
monitoring/grafana/provisioning/dashboards/dashboards.yml
monitoring/grafana/provisioning/dashboards/lottery-overview.json
```

The dashboard JSON is editable in the Grafana UI. Provisioned dashboard files remain the source of truth: if you want a
change to survive container recreation and remain in Git, export/download the dashboard JSON and save it back into the
repository's provisioning directory.

### 12.2 Understand the dashboard layout

The starter dashboard contains panels for:

- HTTP request rate
- Average HTTP request latency
- HTTP p95 latency
- Hikari active connections
- Hikari idle connections
- Hikari threads waiting for a DB connection
- Outbox events published per second
- Outbox publish failures per second
- PostgreSQL connections to the lottery database
- PostgreSQL transaction commits per second
- JVM heap usage

Some metrics only appear after the relevant workload has run. A panel showing `No data` can mean no requests have
happened yet, the application has not exposed that metric, or the Prometheus scrape has not succeeded.

### 12.3 Create a panel manually

1. In Grafana, open **Dashboards → Lottery Overview**.
2. Select **Edit** (or open a new dashboard with **New → New dashboard**).
3. Click **Add → Visualization**.
4. Select the **Prometheus** data source.
5. Switch to **Code** mode if the query editor opens in Builder mode.
6. Enter a PromQL query from the examples below.
7. Choose a visualization:
    - **Time series** for rates, latency and connections over time.
    - **Stat** for a current value or one-minute rate.
    - **Gauge** for a value with a meaningful fixed limit, such as pool usage.
    - **Table** for grouped labels or diagnostic details.
8. Set the panel title, unit, legend and thresholds in the right-side options.
9. Click **Apply**, then **Save dashboard**.

### 12.4 Useful PromQL queries

**HTTP requests per second**

```promql
sum(rate(http_server_requests_seconds_count{application="fixed-probability-lottery-async", uri=~"/api/lottery/.*"}[1m]))
```

**Average HTTP latency in milliseconds**

```promql
1000 *
sum(rate(http_server_requests_seconds_sum{application="fixed-probability-lottery-async", uri=~"/api/lottery/.*"}[5m]))
/
clamp_min(sum(rate(http_server_requests_seconds_count{application="fixed-probability-lottery-async", uri=~"/api/lottery/.*"}[5m])), 1e-9)
```

**HTTP p95 latency in milliseconds** (requires the histogram setting enabled in `application.yml`)

```promql
1000 * histogram_quantile(
  0.95,
  sum by (le) (
    rate(http_server_requests_seconds_bucket{application="fixed-probability-lottery-async", uri=~"/api/lottery/.*"}[5m])
  )
)
```

**Hikari active connections**

```promql
hikaricp_connections_active{pool="LotteryHikariPool"}
```

**Hikari idle connections**

```promql
hikaricp_connections_idle{pool="LotteryHikariPool"}
```

**Threads waiting for a Hikari connection**

```promql
hikaricp_connections_pending{pool="LotteryHikariPool"}
```

**Outbox publish throughput**

```promql
sum(rate(lottery_outbox_published_total[1m]))
```

**Outbox publish failures per second**

```promql
sum(rate(lottery_outbox_publish_failures_total[5m]))
```

**PostgreSQL connections to the lottery database**

```promql
pg_stat_database_numbackends{datname="lottery"}
```

**PostgreSQL transaction commits per second**

```promql
rate(pg_stat_database_xact_commit{datname="lottery"}[5m])
```

**JVM heap used (MiB)**

```promql
sum(jvm_memory_used_bytes{area="heap", application="fixed-probability-lottery-async"}) / 1024 / 1024
```

If a metric returns no series, open `http://localhost:9090/targets` and confirm both scrape targets are **UP**. Then
inspect `http://localhost:8080/actuator/prometheus` or `http://localhost:9187/metrics` and search for the metric name.
Micrometer converts dotted counter names to Prometheus names with underscores and a `_total` suffix for counters.

### 12.5 Suggested panel units and thresholds

| Panel                       | Visualization | Unit / guidance                                             |
|-----------------------------|---------------|-------------------------------------------------------------|
| HTTP request rate           | Time series   | requests/sec                                                |
| Average / p95 latency       | Time series   | milliseconds; set a threshold based on your SLO             |
| Hikari active / idle        | Time series   | connections                                                 |
| Hikari pending              | Time series   | threads; sustained values above zero indicate pool pressure |
| Outbox published / failures | Time series   | events/sec                                                  |
| PostgreSQL connections      | Time series   | connections; compare with server capacity                   |
| PostgreSQL commits          | Time series   | transactions/sec                                            |
| JVM heap                    | Time series   | MiB; compare with configured JVM memory                     |

Avoid choosing arbitrary production alert thresholds. First run a representative workload, observe normal behavior, and
define thresholds from service-level objectives and capacity tests.

### 12.6 Save and export your dashboard

1. Click **Save dashboard** (disk icon).
2. Enter a dashboard name and save.
3. To export: open the dashboard menu and choose **Share → Export** (wording can vary by Grafana version).
4. Choose **Export as JSON** and save the JSON into `monitoring/grafana/provisioning/dashboards/`.
5. Commit the JSON file to Git so the dashboard can be recreated with the project.

### 12.7 Important monitoring limitation: outbox backlog

The current app emits outbox claimed, published and failed counters. The starter dashboard does **not** expose a live
pending-row count or oldest-pending-event age as a Prometheus metric. Those require an explicit custom gauge/collector
or a suitable database exporter custom query. Do not mistake publish counters for queue depth. For operational
monitoring, add gauges for:

- `lottery_outbox_pending_count`
- `lottery_outbox_oldest_pending_age_seconds`

These can be collected with a scheduled, lightweight query against `lottery_outbox`. Keep the query efficient and avoid
polling the database too frequently.

## 13. PostgreSQL operational checks

Connect:

```bash
psql -h localhost -p 5433 -U lottery -d lottery
```

Current connections:

```sql
SELECT state, COUNT(*)
FROM pg_stat_activity
GROUP BY state
ORDER BY state;
```

Long-running active statements:

```sql
SELECT pid,
       now() - query_start AS duration,
       wait_event_type,
       wait_event,
       LEFT(query, 160) AS query
FROM pg_stat_activity
WHERE state = 'active'
  AND pid <> pg_backend_pid()
ORDER BY query_start;
```

Table scans and dead tuples:

```sql
SELECT relname, seq_scan, idx_scan, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
ORDER BY seq_scan DESC;
```

Lock waits:

```sql
SELECT pid, wait_event_type, wait_event, LEFT(query, 160) AS query
FROM pg_stat_activity
WHERE wait_event_type = 'Lock';
```

Pending outbox rows and oldest pending age:

```sql
SELECT COUNT(*) AS pending_count,
       now() - MIN(created_at) AS oldest_pending_age
FROM lottery_outbox
WHERE published_at IS NULL;
```

## 14. Scaling and reliability notes

- **API replicas:** stateless API instances can be scaled horizontally, but each instance adds a Hikari pool. Budget
  total connections, not just per-container connections.
- **Kafka consumers:** scale consumer instances/concurrency based on topic partition count, processing time and lag. In
  one consumer group, active parallelism is bounded by the number of partitions.
- **Outbox publishers:** multiple publishers can safely claim disjoint rows with `FOR UPDATE SKIP LOCKED`. Lease
  duration must exceed normal processing time or be renewed safely.
- **At-least-once delivery:** duplicates are expected in failure windows. Keep consumers idempotent.
- **Limited inventory:** strict quota enforcement creates contention on the inventory row for a scarce prize. Do not
  remove the atomic conditional update if the quota must never be exceeded.
- **Read replicas:** may help read-heavy workloads, but replication lag means they should not be used for
  correctness-critical inventory claims or reads requiring immediate consistency.
- **PgBouncer / partitioning / sharding:** consider only after measuring actual bottlenecks and query patterns.
- **Monitoring:** watch p95/p99 latency, error rate, Hikari pending threads, active connections, PostgreSQL locks, CPU,
  disk I/O, transaction rate, outbox backlog and Kafka consumer lag.

## 15. Production hardening checklist

Before production, add and verify:

- Authentication and authorization; derive `userId` from the authenticated principal.
- API rate limits and abuse prevention.
- TLS and secret management; do not use committed `.env` values.
- Multi-node PostgreSQL/Kafka deployment, backups, restore drills and disaster recovery.
- Kafka retention, replication, partition strategy, consumer lag alerts and dead-letter/replay procedures.
- Outbox backlog/oldest-age metrics and alerting.
- Tests for publisher retry, crash-after-publish, duplicate events, lease expiry, consumer restart and quota contention.
- End-to-end load tests that poll to completion and report both submission and completion latency.
- Structured logs, tracing, dashboards, SLOs and operational runbooks.

## 16. Troubleshooting

**App cannot connect to PostgreSQL**

```bash
docker compose ps
docker compose logs postgres
docker compose logs app
```

Check `DB_PASSWORD` in `.env` and confirm the PostgreSQL health check passes.

**Kafka is not ready**

```bash
docker compose logs kafka
docker compose logs app
```

The app uses `kafka:9092` inside the Compose network. Use `localhost:9092` only from the host machine.

**Grafana shows no data**

1. Check `docker compose ps`.
2. Open `http://localhost:9090/targets`; both `lottery-app` and `postgres` should be `UP`.
3. Open `http://localhost:8080/actuator/prometheus` and confirm app metrics exist.
4. Open `http://localhost:9187/metrics` and confirm exporter metrics exist.
5. In Grafana, confirm the Prometheus data source URL is `http://prometheus:9090` (container-to-container address).

**The `.env.example` file is not visible**

It is a hidden dotfile. In macOS Finder, press **Command + Shift + .** to show hidden files, or use Terminal:

```bash
ls -la
```

**Reset all local state**

```bash
docker compose down -v
docker compose up --build -d
```

This deletes local PostgreSQL and Grafana volumes.
