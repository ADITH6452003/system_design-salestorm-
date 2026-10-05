# SALESTORM Architecture: SOLID Principles & Design Patterns
## Low-Level Design (LLD) & Object-Oriented Blueprint

---

## 1. Overview

The **SALESTORM** flash sale backend is built using clean, modular object-oriented design principles. By decoupling domain logic from infrastructure details, the codebase guarantees sub-50ms execution times under 10,000 req/sec while remaining easy to test, maintain, and scale.

---

## 2. SOLID Principles Breakdown

### 1. Single Responsibility Principle (SRP)
> *"A class should have one, and only one, reason to change."*

Each core service in SALESTORM handles a distinct domain responsibility:

* **`InventoryService`:** Responsible **only** for stock check-and-decrement execution within Redis memory shards. It contains zero payment logic or database persistence code.
* **`PaymentService`:** Responsible **only** for idempotency verification and executing charge processing via payment gateway adapters.
* **`OutboxRepository`:** Responsible **only** for writing event records to the local PostgreSQL Write-Ahead Log (WAL) outbox table.

---

### 2. Open / Closed Principle (OCP)
> *"Software entities should be open for extension, but closed for modification."*

SALESTORM uses interface-driven abstractions for third-party integrations:

* **`IPaymentGateway` Interface:** Integrations like Stripe or Razorpay implement a common gateway interface (`process_charge`). Adding a new payment provider requires writing a new class (extension) without modifying existing `PaymentService` orchestration logic (closed for modification).

---

### 3. Liskov Substitution Principle (LSP)
> *"Derived classes must be completely substitutable for their base types."*

Concrete repository classes fulfill interface contracts seamlessly:

* Both **`RedisIdempotencyRepository`** (used in local development/testing) and **`DynamoDBIdempotencyRepository`** (used in AWS production) implement the abstract **`IIdempotencyRepository`** interface.
* The higher-level `PaymentService` can accept either repository instance without breaking runtime behavior or requiring conditional environment logic.

---

### 4. Interface Segregation Principle (ISP)
> *"Clients should not be forced to depend upon interfaces they do not use."*

Instead of creating monolithic repositories with unrelated CRUD methods, interfaces are strictly fine-grained:

* **`IStockHoldReader`:** Exposes read-only inventory checks.
* **`IStockHoldWriter`:** Exposes atomic decrement methods.
* **`IOutboxPublisher`:** Exposes outbox event insertion methods.

---

### 5. Dependency Inversion Principle (DIP)
> *"High-level modules should not depend on low-level modules; both should depend on abstractions."*

High-level application controllers depend strictly on interfaces injected at runtime:

* **`FlashSaleController`** takes `IInventoryService` and `IPaymentService` as constructor arguments. It does not instantiate low-level Redis or PostgreSQL clients directly, allowing full unit-testing isolated from physical infrastructure.

---

## 3. Design Patterns Applied
┌─────────────────────────────────────────────────────────────────────────┐
│                           SALESTORM PATTERN MAP                         │
└─────────────────────────────────────────────────────────────────────────┘
│
├── Data & Resilience Patterns
│    ├── Transactional Outbox Pattern  ───► Guarantees ACID DB + Event atomic commits
│    └── Change Data Capture (CDC)    ───► Zero-overhead log streaming to Kafka
│
├── Structural & Routing Patterns
│    ├── Strategy Pattern             ───► MD5 Hash Router across 10 Redis shards
│    └── Repository Pattern           ───► Hides SQL/Redis implementation details
│
└── Behavioral Patterns
├── Command Pattern              ───► Encapsulates atomic Redis Lua scripts
└── Circuit Breaker Pattern      ───► Prevents cascading gateway failure timeouts


### 1. Transactional Outbox Pattern (Data Consistency)
* **Problem:** Updating PostgreSQL and publishing a Kafka event in separate steps creates a dual-write hazard if the network drops between operations.
* **Solution:** The business record (`payments`) and the event payload (`outbox_events`) are inserted into PostgreSQL within the **same local ACID transaction**.

### 2. Change Data Capture / CDC Pattern (Asynchronous Event Pipeline)
* **Problem:** Polling the database for new outbox events creates heavy CPU overhead under 10,000 RPS.
* **Solution:** **Debezium** tail-reads PostgreSQL's Write-Ahead Log (WAL) directly at the storage level and pushes events asynchronously to Apache Kafka without touching application threads.

### 3. Strategy Pattern (In-Memory Hash Routing)
* **Problem:** Single Redis counters bottleneck under extreme concurrency.
* **Solution:** Encapsulates shard allocation logic into a `ShardRouterStrategy`. The `MD5HashShardingStrategy` deterministically assigns user IDs across 10 inventory shards (`stock_shard_0` through `stock_shard_9`).

### 4. Command Pattern (Atomic Memory Execution)
* **Problem:** Read-then-decrement operations in RAM produce race conditions.
* **Solution:** Wraps stock verification, decrement, and reservation creation into an encapsulated Lua script object (`ReserveStockCommand`) executed single-threadedly by Redis.

### 5. Circuit Breaker Pattern (Fault Tolerance)
* **Problem:** Slow external payment gateways can exhaust application thread pools.
* **Solution:** Wraps external gateway network calls in a Circuit Breaker. If failure thresholds are exceeded, the circuit opens to fail fast (`HTTP 503`) without locking up server memory.

---

## 4. Class & Interface Specification Code

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple

# --- DIP & OCP: ABSTRACT INTERFACES ---

class IInventoryService(ABC):
    @abstractmethod
    def reserve_stock(self, user_id: str, ttl_seconds: int = 600) -> Tuple[int, int, str]:
        pass

class IPaymentGateway(ABC):
    @abstractmethod
    def charge(self, user_id: str, amount: float) -> Dict[str, Any]:
        pass

class IIdempotencyRepository(ABC):
    @abstractmethod
    def acquire_lock(self, key: str, ttl_seconds: int = 300) -> bool:
        pass

    @abstractmethod
    def get_cached_response(self, key: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def save_response(self, key: str, payload: Dict[str, Any], ttl_seconds: int = 300) -> None:
        pass


# --- CONCRETE IMPLEMENTATIONS ---

class StripePaymentGateway(IPaymentGateway):
    """Open for Extension: New payment providers implement IPaymentGateway"""
    def charge(self, user_id: str, amount: float) -> Dict[str, Any]:
        return {"txn_id": "stripe_txn_1029", "status": "SUCCESS"}


class FlashSaleController:
    """SRP & DIP: Controller relies strictly on abstract interfaces"""
    def __init__(self, inventory_svc: IInventoryService, payment_gateway: IPaymentGateway):
        self.inventory_svc = inventory_svc
        self.payment_gateway = payment_gateway
