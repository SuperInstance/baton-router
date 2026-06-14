# The Baton Router — Making Agents Talk Across the Void

Loom built a baton protocol where agents communicate by committing messages to git. It works. It's slow. This is the fast version.

---

## The Problem with Git Batons

Loom's baton system is clever — agents write JSON messages into commits, push, and other agents pull and react. It's durable by definition (git doesn't forget). But it has a fundamental timing problem: polling latency. An agent commits a baton, and the recipient doesn't see it until their next pull cycle. In agent-time, where decisions happen in milliseconds and race conditions live in the gaps between polls, that's an eternity.

Worse, git batons are fire-and-forget. No delivery confirmation. No priority. No replay with filtering. If an agent restarts mid-baton, the message might get processed twice or not at all. There's no dead letter queue for batons that nobody picked up. The conservation audit log is whatever you can reconstruct from `git log --grep`.

The baton protocol got us started. The Baton Router gets us to production.

---

## The Upgrade Path

```
Git Batons                    Baton Router
─────────────                 ──────────────
git commit + push     →       POST /send (instant)
git pull + parse       →       GET /inbox/:agent_id (real-time)
"did they get it?"     →       POST /ack/:message_id (confirmed)
git log --grep          →       GET /replay/:agent_id?since= (filtered)
no priority             →       {-1, 0, 1} priority levels
no retry                →       Queue consumer with 3 retries + DLQ
no rate limiting        →       KV-backed rate limiting per IP
```

The mental model stays the same: agents send batons, agents receive batons. But the transport layer moves from filesystem polling to Cloudflare's edge network. Messages traverse the globe in under 100ms instead of waiting for the next git fetch cycle.

---

## Conservation-Aware Priority

Not all messages are equal. The Baton Router implements a ternary priority system that maps directly to Loom's conservation principles:

| Priority | Meaning         | Behavior                                        |
| -------- | --------------- | ----------------------------------------------- |
| `+1`     | **Urgent**      | Jump the queue. Processed first. PID updates, critical alerts. |
| `0`      | **Normal**      | Standard delivery. Most I2I communication.      |
| `-1`     | **Deferred**    | Low priority. Conservation audits, telemetry. Processed when the system is quiet. |

This isn't just QoS — it's conservation at the protocol level. When the system is under load, deferred messages naturally fall behind. Urgent messages skip the line. The scheduler doesn't need to guess; the priority is encoded in the message envelope itself.

Loom's `CONSERVATION_AUDIT` batons travel at -1. They're important but not time-sensitive. `PID_UPDATE` batons travel at +1 because a drifting process controller needs immediate correction. The priority is part of the message contract.

---

## Replay — The Durability Guarantee

Every message that passes through the Baton Router is written to D1 before it's queued for delivery. The `message_log` table is an append-only event sourcing log:

```
created → queued → delivered → acked
                  ↘ expired → dead_letter
```

If an agent crashes and restarts, it calls `GET /replay/:agent_id?since=<timestamp>` to replay everything it missed. The log records every lifecycle event — when it was created, when it was delivered, when it was acked, and if needed, when it was replayed. This is the durability guarantee that git batons had but couldn't query efficiently. Now it's a SQL query.

Replay isn't just for crash recovery. It's also for debugging — "what did the oracle agent see last Tuesday between 2pm and 3pm?" — and for auditing — "prove that the conservation audit was delivered to crab before the PID drift was detected."

---

## Architecture

```
  Agent A                    Agent B                    Agent C
    │                          │                          │
    │ POST /send               │ GET /inbox               │ GET /replay
    ▼                          ▼                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Baton Router Worker                       │
│                                                              │
│  ┌─────────────┐   ┌───────────────┐   ┌────────────────┐   │
│  │  D1 Insert  │──▶│  Queue Send   │   │  Rate Limiter  │   │
│  │  (durable)  │   │  (async)      │   │  (KV-backed)   │   │
│  └─────────────┘   └───────┬───────┘   └────────────────┘   │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │  Queue Consumer │                        │
│                   │  (3 retries)    │                        │
│                   └────────┬────────┘                        │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │  D1 Update      │                        │
│                   │  + Event Log    │                        │
│                   └────────┬────────┘                        │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │  Dead Letter    │                        │
│                   │  (on failure)   │                        │
│                   └─────────────────┘                        │
└──────────────────────────────────────────────────────────────┘
```

**Flow:** A producer agent calls `POST /send`. The message is immediately written to D1 (durable storage). It's then enqueued via Cloudflare Queues for async delivery. The queue consumer attempts delivery, updates the message status, and logs every event. If delivery fails after 3 retries, the message lands in the dead letter table.

The consumer agent polls `GET /inbox/:agent_id` and receives messages ordered by priority. It acknowledges receipt via `POST /ack/:message_id`. The cycle is complete.

---

## API Walkthrough

### Register an Agent

```bash
curl -X POST https://baton-router.workers.dev/register \
  -H 'Content-Type: application/json' \
  -d '{"name":"loom-1","role":"loom","public_key":"ssh-ed25519 AAAA..."}'
```

Returns an `agent_id` and a one-time `api_key`. Store the key — it's hashed on disk.

### Send a Baton

```bash
curl -X POST https://baton-router.workers.dev/send \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: btr_...' \
  -d '{
    "from_agent": "loom-1",
    "to_agent": "oracle-1",
    "type": "PID_UPDATE",
    "payload": "{\"kp\":2.5,\"ki\":0.8,\"setpoint\":42}",
    "priority": 1
  }'
```

Priority 1 = urgent. The oracle needs this now.

### Check Inbox

```bash
curl https://baton-router.workers.dev/inbox/agent-uuid \
  -H 'X-API-Key: btr_...'
```

Messages sorted by priority: urgent first, then normal, then deferred.

### Acknowledge

```bash
curl -X POST https://baton-router.workers.dev/ack/message-uuid \
  -H 'X-API-Key: btr_...'
```

Confirms receipt. The message lifecycle is now `created → delivered → acked`.

### Replay History

```bash
curl 'https://baton-router.workers.dev/replay/agent-uuid?since=2025-06-14T00:00:00Z' \
  -H 'X-API-Key: btr_...'
```

Every message since the timestamp, in order. For crash recovery and audit trails.

### View Stats

```bash
curl https://baton-router.workers.dev/stats
```

Delivery rates, dead letter count, message breakdown by type and priority.

---

## Message Types

| Type                 | Purpose                                          | Typical Priority |
| -------------------- | ------------------------------------------------ | ---------------- |
| `I2I`                | Inter-instance communication (general)           | 0                |
| `GC_SYNC`            | Garbage collection synchronization               | 0                |
| `PID_UPDATE`         | Process controller parameter updates             | +1               |
| `CONSERVATION_AUDIT` | Conservation law audit checkpoints               | -1               |
| `BOTTLE`             | Message-in-a-bottle (delayed/asymmetric)         | 0                |
| `SPLINE`             | Curve-fitting / trajectory messages              | 0                |

These map directly to Loom's baton taxonomy. New types require a schema migration (by design — the CHECK constraint enforces the contract).

---

## Quick Start

```bash
# Clone and install
git clone <repo-url> baton-router
cd baton-router
npm install

# Create D1 database
npx wrangler d1 create baton-router-db
# Update database_id in wrangler.toml with the returned ID

# Create KV namespace
npx wrangler kv namespace create RATE_LIMIT
# Update the KV id in wrangler.toml

# Create the queue
npx wrangler queues create baton-queue
npx wrangler queues create baton-dlq

# Initialize schema
npm run db:init

# Set admin secret (for route management)
npx wrangler secret put ROUTER_API_KEY

# Deploy
npm run deploy

# Register your first agent
curl -X POST https://baton-router.<account>.workers.dev/register \
  -H 'Content-Type: application/json' \
  -d '{"name":"loom-1","role":"loom"}'

# Send a message
curl -X POST https://baton-router.<account>.workers.dev/send \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: btr_...' \
  -d '{"from_agent":"loom-1","to_agent":"loom-1","type":"I2I","payload":"hello world","priority":0}'

# Check stats
curl https://baton-router.<account>.workers.dev/stats
```

---

## Roles

Five canonical agent roles, matching the Loom hierarchy:

- **forge** — Builds and compiles. The maker.
- **loom** — Weaves processes together. The coordinator.
- **oracle** — Predicts and observes. The seer.
- **ship** — Deploys and delivers. The mover.
- **crab** — Audits and conserves. The guardian.

Each role has semantic meaning in the baton network. The `CONSERVATION_AUDIT` type routes to `crab` by default. `PID_UPDATE` routes to `oracle`. The routing table is editable via `POST /routes`.

---

## The Point

Loom proved that agents can coordinate through git commits. It's a beautiful hack — durable, auditable, deterministic. But it's slow, and slow kills real-time systems.

The Baton Router takes the same contract — typed messages, priority, delivery confirmation, audit trail — and runs it on Cloudflare's edge. Same semantics. 100ms instead of 30s. SQL replay instead of `git log --grep`. A dead letter queue instead of silent drops.

The baton protocol didn't need to be replaced. It needed infrastructure.

---

*baton-router v1.0 — Built for the Loom agent network.*
