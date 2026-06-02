# Kafka Scenarios — Test Plan & Summary

This document lists **all Kafka scenarios** we want to try in this solution: what each scenario proves, which project to use, how to run it, and current status.

**Related docs:**
- [Kafka-Partition-Summary.md](./Kafka-Partition-Summary.md) — partition sizing in production

---

## Projects map

| Project | Role | Notes |
|---|---|---|
| **KafkaProducer1** | Demo producer | `produce`, `stream`, `stream-auto` |
| **KafkaConsumer1** | Simple consumer | Auto commit, console loop |
| **KafkaConsumer2** | Production-style consumer | Host, manual commit, handlers |
| **BankingProducer** | Banking events producer | Key = `accountId`, sequence numbers *(planned)* |
| **BankingConsumer1** | Banking ledger consumer #1 | Same group as BankingConsumer2 *(planned)* |
| **BankingConsumer2** | Banking ledger consumer #2 | Failover / rebalance demo *(planned)* |

**Aiven topic (dev):** `demo-message-topic` (or dedicated `banking-events` topic for banking scenarios)

**Prerequisites:**
- `appsettings.Development.json` configured (gitignored)
- Bootstrap server includes **host:port**
- CA certificate path valid; AVG HTTPS scanning disabled or Aiven excluded

---

## Status legend

| Status | Meaning |
|---|---|
| ✅ Done | Implemented and exercised |
| 📖 Learned | Concept covered in discussion / docs |
| 🔲 Planned | Not yet implemented in code |
| ⏳ Partial | Started but not complete |

---

## A. Connection & configuration

### A1 — Connect to Aiven with SASL_SSL ✅

**Goal:** Producer and consumer reach the broker with SCRAM + CA cert.

**Projects:** KafkaProducer1, KafkaConsumer1

**Steps:**
```powershell
cd KafkaProducer1
dotnet run -- produce "ping"

cd ..\KafkaConsumer1
dotnet run
```

**Expected:** No SSL/auth errors; message appears on consumer.

**Notes:** Missing port → defaults to 9092. AVG TLS interception breaks cert verification.

---

### A2 — Environment-based config ✅

**Goal:** `appsettings.Development.json` overrides template; env vars override JSON.

**Projects:** All

**Steps:**
```powershell
# Default Development
dotnet run

# Force Production template
$env:DOTNET_ENVIRONMENT = "Production"
dotnet run
```

**Expected:** Dev uses real credentials; Production uses placeholders unless env vars set.

---

### A3 — Secrets not committed ✅

**Goal:** Local dev settings stay out of git.

**Pattern:** `**/appsettings.Development.json` in `.gitignore`

---

## B. Basic messaging

### B1 — One-shot produce / consume ✅

**Goal:** Send explicit messages and read them once.

**Projects:** KafkaProducer1 + KafkaConsumer1

**Steps:**
```powershell
# Terminal 1
cd KafkaConsumer1
dotnet run

# Terminal 2
cd KafkaProducer1
dotnet run -- produce "Hello" "World"
```

**Expected:** Consumer prints both messages with topic/partition/offset.

---

### B2 — Interactive stream ✅

**Goal:** Type messages manually until `exit`.

**Projects:** KafkaProducer1 (`stream`)

```powershell
dotnet run -- stream
```

---

### B3 — Auto stream (burst simulation) ✅

**Goal:** Send many messages quickly to observe consumer catch-up and lag.

**Projects:** KafkaProducer1 (`stream-auto`) + KafkaConsumer1

```powershell
dotnet run -- stream-auto 1
```

**Expected:** Producer sends every N seconds; consumer may fall behind if slow.

---

### B4 — Verify messages in Aiven console 📖

**Goal:** Confirm messages exist in the topic and consumer group lag is visible.

**Steps:**
1. Aiven → Kafka service → **Topics** → your topic → **Messages** / fetch
2. **Consumer groups** → check offset and lag

**Expected:** Produced messages visible; lag decreases as consumer catches up.

---

## C. Consumer patterns

### C1 — Simple consumer (auto commit) ✅

**Goal:** Understand default `EnableAutoCommit = true` behavior.

**Project:** KafkaConsumer1

**Observe:** No `Commit()` in code; offsets saved by client in background.

---

### C2 — Production consumer (manual commit) ✅

**Goal:** Commit only after handler succeeds; rebalance logging.

**Project:** KafkaConsumer2

```powershell
cd KafkaConsumer2
dotnet run
```

**Expected:** Structured logs; partition assigned/revoked on start/stop.

---

### C3 — Compare auto vs manual commit 🔲

**Goal:** See duplicate / redelivery risk with auto commit vs manual on crash.

**Projects:** KafkaConsumer1 vs KafkaConsumer2

**Steps:**
1. Start consumer, produce several messages
2. Kill consumer mid-processing (Ctrl+C or kill process)
3. Restart and note whether last message repeats

**Expected:** Manual commit → redeliver uncommitted; auto commit → smaller window but less precise.

---

### C4 — Graceful shutdown 📖 / ⏳

**Goal:** Finish in-flight message, commit, then exit on SIGTERM/Ctrl+C.

**Project:** KafkaConsumer2 (extend if needed)

**Expected:** No unnecessary redelivery after clean shutdown.

---

## D. Offsets, restart, and lag

### D1 — Resume after consumer stop ✅ 📖

**Goal:** Consumer restarts from last **committed** offset, not from beginning.

**Steps:**
1. Run consumer, produce messages 1–5, stop consumer
2. Produce 6–10 while consumer down
3. Start consumer again

**Expected:** Consumer continues from last committed offset (may read 6–10 backlog, not re-read 1–5 unless not committed).

---

### D2 — Producer faster than consumer 📖

**Goal:** Observe growing **consumer lag** when send rate > process rate.

**Steps:** `stream-auto` with 0.5s interval + slow handler (e.g. `Thread.Sleep` in consumer)

**Expected:** Lag increases in Aiven metrics; messages not lost while within retention.

---

### D3 — New consumer group starts fresh 📖

**Goal:** Different `GroupId` = separate offset tracker.

**Steps:** Change `GroupId` in config, restart consumer with `AutoOffsetReset = Earliest`

**Expected:** Reads from beginning of topic (first time for that group).

---

## E. Ordering & consumer groups

### E1 — Order within one partition 📖 ✅

**Goal:** Messages on same partition consumed in offset order (1, 2, 3…).

**Setup:** Single partition topic or same message key.

---

### E2 — No global order across partitions 📖

**Goal:** Messages on P0 and P1 can be interleaved at consumer.

**Setup:** Topic with 2+ partitions, unkeyed messages.

**Expected:** Order per partition only.

---

### E3 — Same GroupId, two consumers (1 partition) 📖

**Goal:** Only one active reader; second is standby; order preserved.

**Projects:** BankingConsumer1 + BankingConsumer2 (same group) or two KafkaConsumer1 instances with same config

**Steps:**
1. Start Consumer1 and Consumer2 with **same** `GroupId`, topic with **1 partition**
2. Produce messages
3. Stop Consumer1; observe Consumer2 takes over
4. Produce more; restart Consumer1

**Expected:**
- Only one consumes at a time
- Order preserved on partition
- Resume from group committed offset

---

### E4 — Same GroupId, two consumers (multiple partitions) 🔲

**Goal:** Work split by partition; order per partition only.

**Setup:** Topic with 3+ partitions, same group, two consumer instances.

**Expected:** Each consumer assigned different partitions.

---

### E5 — Different GroupId, two consumers 📖

**Goal:** Each group reads **all** messages independently.

**Setup:** KafkaConsumer1 (`demo-consumer-group`) vs KafkaConsumer2 (`demo-consumer-group-v2`)

**Expected:** Both receive every message; separate lag/offsets.

---

## F. Banking scenarios (planned)

These scenarios use **BankingProducer**, **BankingConsumer1**, **BankingConsumer2** with account-keyed events.

### F1 — Per-account chronological order 🔲

**Goal:** All events for `ACC-001` stay in order (deposit → transfer → withdraw).

**Design:**
- Message **key** = `accountId`
- Payload: `{ accountId, sequence, eventType, amount, timestamp }`
- Topic: 6–12 partitions (recommended) or 1 partition (strict global order)

**Steps:**
1. Produce ordered sequence for ACC-001 and ACC-002 (interleaved sends)
2. Consume and verify sequence per account

**Expected:** Each account’s sequence is strictly increasing; accounts can interleave.

---

### F2 — Sequence gap detection 🔲

**Goal:** Detect missing event (e.g. expected sequence 5, received 6).

**Design:** In-memory or DB “last sequence per account”; log/alert on gap.

---

### F3 — Idempotent processing (duplicate event) 🔲

**Goal:** Same `eventId` processed twice does not double-post to ledger.

**Design:** Store processed `eventId`; skip duplicates.

**Steps:** Replay same offset or redeliver after crash.

---

### F4 — Banking failover (Consumer2 crash) 🔲

**Goal:** Consumer2 crashes mid-stream; Consumer1 (same group) continues same partitions in order.

**Projects:** BankingConsumer1 + BankingConsumer2, same `GroupId`

**Steps:**
1. Both running; produce events for multiple accounts
2. Kill BankingConsumer2
3. Observe rebalance → BankingConsumer1 takes partitions
4. Produce more events; restart BankingConsumer2

**Expected:** No reorder within partition; possible duplicate without idempotency (F3).

---

### F5 — High-volume burst per account 🔲

**Goal:** Many events for one account in short time; consumer keeps sequence order.

**Steps:** `stream-auto` style producer keyed to one account.

---

## G. Operations & production practices

### G1 — Partition sizing 📖

**Goal:** Choose partition count for throughput vs ordering.

**See:** [Kafka-Partition-Summary.md](./Kafka-Partition-Summary.md)

---

### G2 — Monitor lag in Aiven 🔲

**Goal:** Alert when lag exceeds threshold.

---

### G3 — Dead letter queue (DLQ) 🔲

**Goal:** Poison message does not block partition forever.

**Design:** After N failures, publish to `banking-events-dlq` and commit offset.

---

### G4 — Keyed producer in KafkaProducer1 / BankingProducer 🔲

**Goal:** Demonstrate `Message.Key` affecting partition assignment.

---

## Quick command reference

```powershell
# Producer
cd KafkaProducer1
dotnet run -- produce "msg"
dotnet run -- stream
dotnet run -- stream-auto 2

# Consumers
cd KafkaConsumer1
dotnet run

cd KafkaConsumer2
dotnet run

# Banking (when implemented)
cd BankingProducer
dotnet run

cd BankingConsumer1
dotnet run

cd BankingConsumer2
dotnet run
```

---

## Suggested order to try remaining scenarios

1. **F1** — Banking keyed producer + per-account order  
2. **E3** — Same group, 1 partition, failover with BankingConsumer1/2  
3. **F3** — Idempotent handler  
4. **C3** — Auto vs manual commit on crash  
5. **E4** — Multi-partition, two consumers  
6. **D2** — Lag under burst  
7. **G3** — DLQ (optional advanced)

---

## Implementation checklist

| Scenario | Code change needed |
|---|---|
| F1–F5 Banking | Implement BankingProducer + BankingConsumer1/2 |
| E3/E4 Failover demo | Run two consumer instances; optional shared group config |
| C3 Auto vs manual | Run existing Consumer1 vs Consumer2 side by side |
| G4 Keyed messages | Add `Key` to producer `ProduceAsync` |
| G3 DLQ | New topic + handler branch in Consumer2 |

---

*Last updated: scenarios from Aiven Kafka demo solution (KafkaProducer1, KafkaConsumer1/2, Banking projects).*
