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

## Coordinator Authority Tradeoff

Raft would also solve coordinator authority. A Raft-backed coordinator group
would elect one leader, replicate control-plane updates through a committed log,
and let follower coordinators stay warm with the same epoch and shard-map state.
That model can support coordinator scaling for read-only/status paths because
followers can serve replicated state locally, while writes still go through the
Raft leader or through a leader-forwarding path.

This branch uses DynamoDB instead for simplicity. DynamoDB conditional writes
give us the main property we need first: exactly one active coordinator lease
holder with a fencing token. That avoids implementing and operating a Raft
cluster while we are still changing ingestion, shard failover, and result
bookkeeping semantics. The tradeoff is that DynamoDB centralizes coordinator
authority in an AWS service; horizontally scaled write routers and durable
ingestion still need SQS-style queueing and later routing work.

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
- `-lease-ttl-seconds <seconds>`, optional lease TTL; defaults to `30`
- `-lease-renew-seconds <seconds>`, optional renewal interval; defaults to `10`
- `-standby`, optional warm standby mode; stay alive as passive when another
  coordinator currently holds the lease

The DynamoDB table uses a single string partition key named `pk`:

- `pk="lease"` stores the active coordinator holder, fencing token, and lease
  expiry timestamp.
- `pk="epoch"` stores epoch metadata and accepting state.
- `pk="shard-config"` stores the shard config version.

Use `aws-eval/07-create-control-table.sh` to create the minimal table when
testing the DynamoDB backend. The helper records the selected table and region
in `aws-eval/.state/env.sh`, but existing smoke and benchmark scripts continue
to use the memory backend unless explicitly configured otherwise.

## Coordinator Role Terms

Warm standby distinguishes three coordinator roles:

- `active`: this coordinator holds a live lease and can admit mutating
  coordinator RPCs after renewing that lease.
- `passive`: this coordinator is alive for read-only status and lease retry, but
  does not currently hold authority.
- `stale`: this coordinator used to hold authority, but lease renewal failed or
  another coordinator acquired a newer fencing token.

`stale` is a debugging signal for the split-brain-risk case. It should behave
like `passive` for mutations: reject `StartEpoch`, reject `Upload1`, and avoid
epoch completion. The fencing token is the safety mechanism; an old token cannot
renew or mutate after a newer holder takes over.

The warm-standby slice has local regression coverage and AWS validation, but it
has not had a deep manual code review. Before merging broadly or building SQS
ingestion on top, review coordinator role transitions, mutation lease gates,
status semantics, and the AWS two-coordinator validation path.

## Active-Passive Shard Health

The coordinator now owns a read-only shard health monitor. It refreshes cached
status for each configured shard pair on a fixed interval:

- active pair health is read through the existing active leader client
- standby pair health is read by dialing the configured standby leader with a
  bounded timeout and calling `Server.Status`
- standby status is reported only when a standby pair is configured
- `Server.Status` includes the leader process state plus peer readiness/error,
  so probing the standby leader is enough to see whether that standby pair is
  basically reachable

`Coordinator.Status` keeps the older `Reachable`, `Status`, and `StatusError`
fields as active-leader compatibility fields. It also reports explicit
`active_*` and `standby_*` health fields with last-checked timestamps.

This loop only detects and reports active/standby health. It does not change
routing, promote a standby, mutate shard assignment state, or write to the
control store.

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
implemented yet. The current code has local and DynamoDB control-store wiring,
lease/fencing enforcement, and active-passive shard health monitoring, but no
active-passive promotion.

Shard health monitoring is a prerequisite for failover, not failover itself.
Shard active/passive promotion still requires durable accepted-work/session
replay, promotion fencing, routing updates, and pair partial-delivery
hardening. Horizontally scaled stateless write routers also remain later work.
