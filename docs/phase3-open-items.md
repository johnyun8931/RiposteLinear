# Phase 3 Open Items

Phase 3 verification status as of 2026-05-04:

- verified locally with a live multi-process setup:
  - real coordinator + 2 shards + 2 servers per shard
  - writes rejected before epoch start
  - coordinated `StartEpoch` opens writes across shards with matching epoch metadata
  - shard-local result files are written under a live coordinated epoch
  - deterministic boundary writes land in the expected shard and preserve payload bytes
  - a second coordinated epoch starts cleanly after the first completes
- verified on AWS with a 6-node coordinator + 2-shard topology:
  - smoke test verified deterministic row routing and per-shard result files
  - short client-concurrency sweep showed sharded throughput winning at every tested concurrency
  - long `600s` measured run at `CLIENT_CONCURRENCY=16` and `CLIENT_RETRY_OVERLOAD=1` confirmed aggregate throughput improvement:
    - baseline: `38,908` accepted requests, `64.85 req/sec`
    - sharded: `79,024` accepted requests, `131.71 req/sec`
    - delta: `+40,116` accepted requests, `+103.10%`
  - benchmark artifact: `aws-eval/results/20260504T184429Z-long-c16-retry/comparison-summary.md`

Current rough edges / future work in the first Phase 3 cut:

- `completeEpoch()` only updates coordinator-local state; it does not verify that all shards actually completed
- coordinator `EpochStatus()` currently reports only coordinator-local shared state, not live fanout status from shard leaders
- `StartEpoch` fans out to shard leaders serially today; parallel fanout may be worth adding later if epoch-start latency starts to matter
- coordinator epoch start currently uses all-or-nothing fanout with best-effort rollback on partial failure; it is not a full 2PC/3PC-style durable commit protocol
- current sharding splits routed write traffic by row range, but each shard still allocates and processes the full global table shape (`256x256`); shard 1 stores global rows like `128`, not local row `0`
- true row-local shards would require a later data-layout/protocol change so each shard stores only its assigned row range and maps global rows to shard-local rows
- hammer random mode now generates a fresh random message per upload; deterministic `-x` / `-y` / `-payload` mode remains fixed for targeted verification writes
- `Standby` pair config exists to prepare for future failover work, but coordinator routing currently uses only the active shard leader
- transport/auth still relies on the older certificate/index assumptions from the pre-coordinator architecture
- partial pair-delivery / rollback correctness is still deferred work; if one Riposte server in a shard pair receives a write and the other does not, that failure path is not yet fully hardened
- coordinator/shard health and richer status fanout are deferred to Phase 3.5 before active-passive coordinator failover
- SQS or another durable ingestion queue is deferred because it changes epoch-admission semantics

Keep this list temporary and remove it once the Phase 3.5/Phase 4 planning docs supersede it.
