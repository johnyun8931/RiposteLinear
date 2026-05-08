# AWS-Native Coordinator Failover

This is the Phase 4 direction after parking the Raft coordinator spike on
`archive/raft-coordinator-spike`.

## Architecture Direction

- DynamoDB-style control store is the source of truth for coordinator lease,
  fencing token, epoch metadata, accepting state, and shard config version.
- DynamoDB-backed coordinator session records are an interim durability layer
  for uploads admitted by `Upload1` and not yet finished by `Upload3`.
- SQS-style ingestion queue is the durability and backpressure layer for
  accepted write work in the fuller AWS-native design.
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

- `ControlStore` defines lease/fencing, epoch state, accepting state, and full
  shard config operations.
- `IngestionQueue` defines enqueue, receive, and ack operations for durable
  accepted write/session work.
- In-memory implementations provide deterministic local tests and model the
  DynamoDB/SQS semantics before SDK wiring.
- The coordinator mirrors epoch metadata and accepting state into the local
  control store.
- The coordinator acquires and renews a local fenced lease before admitting new
  `StartEpoch` or `Upload1` mutations.

The current implementation adds opt-in DynamoDB-backed `ControlStore` and
`SessionStore` implementations. The in-memory backends remain the default for
local runs and existing smoke or benchmark scripts.

## DynamoDB Control Store

Coordinator flags:

- `-control-store memory|dynamodb`, default `memory`
- `-control-table <name>`, required when `-control-store dynamodb`
- `-session-store memory|dynamodb`, default `memory`
- `-session-table <name>`, required when `-session-store dynamodb` unless
  `-control-table` is set; defaults to the control table
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
- `pk="shard-config"` stores the authoritative shard topology: version, shard
  count, rows per shard, global table height, and active/standby shard
  assignments. Coordinators may be started with extra `-shard` endpoint
  inventory, but uploads and epoch start use only this active topology.
- `pk="shard-config#epoch#<epoch_id>"` stores the immutable shard topology
  snapshot used by that epoch. The current shard config can change later for a
  future epoch, but historical reads/results should use the epoch snapshot.
- `pk="session#<global_uuid>"` stores a coordinator session admitted by
  `Upload1`, including epoch ID, shard ID, local UUID, hash key, global row, and
  shard-local row. The coordinator deletes this record after successful
  `Upload3`.
- `pk="scaling#epoch#<epoch_id>"` and `pk="scaling#latest"` store completed
  epoch scaling recommendations. These are proposals until an operator applies
  the latest applicable recommendation between epochs.
- `pk="epoch-cycle"` stores the current inter-epoch milestone, such as
  `active`, `recommendation_ready`, `scaling_applied`, or `scaling_skipped`.

The `pk="epoch"` record also stores `shard_config_version` and
`shard_config_key`, where the key points at the epoch-bound shard-config
snapshot.

Manual scaling apply is exposed as an admin RPC through:

```bash
coordinator -admin-target <addr> -dry-run-scaling-recommendation
coordinator -admin-target <addr> -apply-scaling-recommendation
```

Only the current lease holder can apply a recommendation. The apply step rejects
active or accepting epochs, missing/stale/`keep` recommendations, and
recommendations that require shard IDs not present in the configured endpoint
inventory. Successful apply writes a new version of `pk="shard-config"` only;
epoch-bound shard-config snapshots remain immutable.

The AWS-side autoscaler is deliberately not a direct topology writer. It reads
the DynamoDB control records to decide whether a recommendation is worth trying,
then calls the coordinator admin RPC to dry-run or apply it. The coordinator
then re-checks lease authority, epoch state, recommendation freshness, and
configured shard inventory before it writes `pk="shard-config"`.

That boundary keeps one authoritative topology writer:

- DynamoDB stores the truth.
- The coordinator, guarded by the lease, writes epoch/control/topology state.
- The autoscaler observes and requests apply.
- Shard servers process assigned local tables and do not mutate topology.

The epoch-cycle record is not a full workflow engine. It records durable
milestones and gates the next `StartEpoch`: after the first epoch, a new epoch
should start only after the previous recommendation reached `scaling_applied` or
`scaling_skipped`. A later timer-driven scheduler can use those milestones to
run repeated epochs without local operator scripts.

A later design could make the autoscaler the direct topology writer, but only if
the lease/fencing and topology mutation rules move into a shared owner so the
coordinator and autoscaler cannot diverge.

Use `aws-eval/07-create-control-table.sh` to create the minimal table when
testing the DynamoDB backends. The helper records the selected table and region
in `aws-eval/.state/env.sh`, but existing smoke and benchmark scripts continue
to use the memory backends unless explicitly configured otherwise.

## Coordinator Role Terms

Warm standby distinguishes three coordinator roles:

- `active`: this coordinator holds a live lease and can admit mutating
  coordinator RPCs after renewing that lease.
- `passive`: this coordinator is alive for read-only status and lease retry, but
  does not currently hold authority.
- `stale`: this coordinator used to hold authority, but lease renewal failed or
  another coordinator acquired a newer fencing token.

`stale` is a debugging signal for the split-brain-risk case. It should behave
like `passive` for control mutations: reject `StartEpoch` and avoid epoch
completion. Upload routing is allowed only when the shared control store still
reports an active accepting epoch, because in-flight upload sessions are now
recoverable through the shared session store. The fencing token is the safety
mechanism; an old token cannot renew or mutate control state after a newer
holder takes over.

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

Coordinator sessions can now be persisted in DynamoDB, so a coordinator can
recover the route/session metadata for uploads that already passed `Upload1`.
This is still not the final ingestion design: SQS remains needed so accepted
write work has durable backpressure and replay semantics beyond the
`Upload1`/`Upload2`/`Upload3` RPC lifecycle.

## Current Boundary

DynamoDB control-store and session-store wiring is opt-in. SQS and S3 SDK calls
are not implemented yet. The current code has local and DynamoDB control-store
wiring, local and DynamoDB session-store wiring, lease/fencing enforcement, and
active-passive shard health monitoring, but no active-passive shard promotion.

Shard health monitoring is a prerequisite for failover, not failover itself.
Shard active/passive promotion still requires durable accepted-work/session
replay, promotion fencing, routing updates, and pair partial-delivery
hardening. Horizontally scaled stateless write routers also remain later work.
