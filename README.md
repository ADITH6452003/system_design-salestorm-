# ⚡ SALESTORM: High-Concurrency & Resilient Flash Sale Architecture

> **SALESTORM Hackathon Submission** | High-Performance, Resilient, & Fault-Tolerant System Architecture for Extreme Concurrency (10,000 req/sec) and Flash Sale Scenarios.

---

## 🛠️ System Overview & Problem Matrix

SALESTORM solves the 4 primary bottlenecks of high-concurrency e-commerce architectures without relying on heavy relational database row locks or single-key memory limits:

| Problem Boundary | Traditional Bottleneck | SALESTORM Solution Architecture |
| :--- | :--- | :--- |
| **1. Concurrency & Stock** | Database row locking (`SELECT FOR UPDATE`) causes deadlocks; single Redis key saturates single-thread CPU limit. | **Horizontal Inventory Sharding (10 shards)** + **Atomic Redis Lua Scripting** for single-threaded check-and-decrement. |
| **2. Expiry & Inventory Reclamation** | Unreliable Redis keyspace notifications cause inventory leakage on abandoned carts. | **Dual-Clock TTL Strategy** + **Kafka-backed Lazy Reclamation Sweeper**. |
| **3. Payment Idempotency** | Network retries and double-clicking cause duplicate payment gateway charges. | **DynamoDB/Redis Conditional Write (`attribute_not_exists`)** locking before gateway invocation. |
| **4. Downstream Resilience** | Synchronous REST calls fail during a 30-second downstream database crash. | **Transactional Outbox Pattern** + **Debezium Change Data Capture (CDC)** + **Apache Kafka Event Buffering**. |

---

## 📐 Architecture & End-to-End Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Client as Client App (10k Concurrency)
    participant GW as API Gateway (Rate Limiter)
    participant Comp as Stateless Compute (FastAPI)
    participant Redis as Redis Cluster (10 Shards)
    participant IdemDB as DynamoDB (Idempotency Store)
    participant PayGW as Payment Gateway
    participant MainDB as Aurora PostgreSQL
    participant CDC as Debezium CDC
    participant Kafka as Apache Kafka Queue
    participant OrderSvc as Order Microservice

    Note over Client, Redis: PHASE 1: ATOMIC INVENTORY RESERVATION (10,000 RPS)
    Client->>GW: POST /api/v1/reserve {user_id}
    GW->>Comp: Forward allowed request (Rate limited at 1k RPS)
    Comp->>Redis: EVALSHA reserve_lua (KEYS: stock_shard_X, reservation:user_id)
    
    rect rgb(240, 248, 255)
        Note over Redis: Single-Threaded Atomic Lua Check & Decr
        alt Stock > 0
            Redis-->>Comp: Return 1 (Hold Confirmed)
            Comp-->>Client: HTTP 201 Created {reservation_id}
        else Stock <= 0
            Redis-->>Comp: Return -1 (Out of Stock)
            Comp-->>Client: HTTP 409 Conflict (Instant Rejection)
        end
    end

    Note over Client, PayGW: PHASE 2: IDEMPOTENT PAYMENT PROCESSING
    Client->>GW: POST /api/v1/pay {idempotency_key, reservation_id}
    GW->>Comp: Process Payment
    Comp->>IdemDB: PutItem (Condition: attribute_not_exists(idempotency_key))
    
    alt Duplicate Request Detected
        IdemDB-->>Comp: ConditionCheckFailedException
        Comp-->>Client: HTTP 200 OK (Return Cached Transaction Response)
    else First Execution
        IdemDB-->>Comp: Lock Granted (Status: PROCESSING)
        Comp->>PayGW: Execute Charge
        PayGW-->>Comp: Payment Confirmed
        
        Note over Comp, MainDB: PHASE 3: TRANSACTIONAL OUTBOX COMMIT
        Comp->>MainDB: BEGIN TRANSACTION
        Comp->>MainDB: INSERT INTO payments VALUES (...)
        Comp->>MainDB: INSERT INTO outbox_events VALUES (...)
        Comp->>MainDB: COMMIT TRANSACTION
        Comp-->>Client: HTTP 202 Accepted
    end

    Note over MainDB, OrderSvc: PHASE 4: ASYNCHRONOUS CDC & RESILIENT ORDER CREATION
    MainDB->>CDC: Read Write-Ahead Log (WAL)
    CDC->>Kafka: Stream Event to topic: payment.events
    Kafka->>OrderSvc: Consume PaymentConfirmed Event
    OrderSvc->>MainDB: INSERT INTO orders VALUES (...)
