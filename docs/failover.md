# AWS-Native Coordinator Failover

This is the Phase 4 direction after parking the Raft coordinator spike on
`archive/raft-coordinator-spike`.

## Architecture Direction

- DynamoDB-style control store is the source of truth for coordinator lease,
  fencing token, epoch metadata, accepting state, and shard config version.
- SQS-style ingestion queue is the durability and backpressure layer for
  accepted write/session work.
- S3 is reserved for published result files, backups, and offline recovery
  artifacts. It is not live coordination storage.

The committed Phase 3.5 health/status RPCs remain useful for observability, but
they do not decide failover by themselves.

## Local Slices

The first AWS-native implementation slices are intentionally SDK-free:

- `ControlStore` defines lease/fencing, epoch state, accepting state, and shard
  config version operations.
- `IngestionQueue` defines enqueue, receive, and ack operations for durable
  accepted write/session work.
- In-memory implementations provide deterministic local tests and model the
  DynamoDB/SQS semantics before SDK wiring.
- The coordinator mirrors epoch metadata and accepting state into the local
  control store.
- The coordinator acquires and renews a local fenced lease before admitting new
  `StartEpoch` or `Upload1` mutations.

The current implementation adds an opt-in DynamoDB-backed `ControlStore`.
The in-memory backend remains the default for local runs and existing smoke
or benchmark scripts.

## DynamoDB Control Store

Coordinator flags:

- `-control-store memory|dynamodb`, default `memory`
- `-control-table <name>`, required when `-control-store dynamodb`
- `-aws-region <region>`, optional AWS SDK region override
- `-coordinator-id <id>`, optional lease holder ID; defaults to `hostname-pid`

The DynamoDB table uses a single string partition key named `pk`:

- `pk="lease"` stores the active coordinator holder, fencing token, and lease
  expiry timestamp.
- `pk="epoch"` stores epoch metadata and accepting state.
- `pk="shard-config"` stores the shard config version.

Use `aws-eval/07-create-control-table.sh` to create the minimal table when
testing the DynamoDB backend. The helper records the selected table and region
in `aws-eval/.state/env.sh`, but existing smoke and benchmark scripts continue
to use the memory backend unless explicitly configured otherwise.

## Go Toolchain Note

This branch uses typed atomics such as `atomic.Bool` and `atomic.Int32`, and the
module now targets Go 1.24. Keep the local and CI toolchains on the declared
Go/toolchain version before running the DynamoDB-backed coordinator code.

## Intended Future Flow

Coordinators will use the control store to acquire/renew a fenced active lease
before making epoch-control decisions. Epoch start/close and accepting state will
be conditional control-store updates, not process-local truth.

Accepted write/session work will be placed on the ingestion queue so a
coordinator crash does not lose already accepted work. Until the queue is wired
into `Upload1` / `Upload2` / `Upload3`, in-flight coordinator-local sessions
remain a known failover limitation.

## Current Boundary

DynamoDB control-store wiring is opt-in. SQS and S3 SDK calls are not
implemented yet. The current code has local and DynamoDB control-store wiring
and lease/fencing enforcement, but no active-passive promotion.

Shard active/passive promotion, pair partial-delivery hardening, and
horizontally scaled stateless write routers remain later work.
