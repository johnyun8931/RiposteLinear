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

## First Local Slice

The first AWS-native implementation slice is intentionally SDK-free:

- `ControlStore` defines lease/fencing, epoch state, accepting state, and shard
  config version operations.
- `IngestionQueue` defines enqueue, receive, and ack operations for durable
  accepted write/session work.
- In-memory implementations provide deterministic local tests and keep current
  coordinator runtime behavior unchanged.

AWS SDK wiring is deferred until the local interfaces and semantics are reviewed.

## Intended Future Flow

Coordinators will use the control store to acquire/renew a fenced active lease
before making epoch-control decisions. Epoch start/close and accepting state will
be conditional control-store updates, not process-local truth.

Accepted write/session work will be placed on the ingestion queue so a
coordinator crash does not lose already accepted work. Until the queue is wired
into `Upload1` / `Upload2` / `Upload3`, in-flight coordinator-local sessions
remain a known failover limitation.

## Current Boundary

No DynamoDB, SQS, or S3 SDK calls are implemented yet. The current slice only
adds compile-safe interfaces, in-memory implementations, and tests.

Shard active/passive promotion, pair partial-delivery hardening, and
horizontally scaled stateless write routers remain later work.
