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
- sharding now treats each shard as adding one full local table of rows to the global dataset; with two shards the coordinator routes global rows `[0,512)` and rewrites shard 1 global row `256` to local row `0`
- each shard still uses the same fixed local table shape internally; runtime-resizable local shard height remains future work
- hammer random mode now generates a fresh random message per upload; deterministic `-x` / `-y` / `-payload` mode remains fixed for targeted verification writes
- `Standby` pair config exists to prepare for future failover work; coordinator status now monitors active/standby health, but routing still uses only the active shard leader
- transport/auth still relies on the older certificate/index assumptions from the pre-coordinator architecture
- partial pair-delivery / rollback correctness is still deferred work; if one Riposte server in a shard pair receives a write and the other does not, that failure path is not yet fully hardened
- coordinator/shard health and richer status fanout have been added for Phase 3.5; see `docs/failover.md`
- Phase 4 is pivoting AWS-native: DynamoDB-style control state, SQS-style durable ingestion, and S3 result/artifact storage
- in-flight coordinator-local sessions remain a failover limitation until ingestion is wired through the queue

Keep this list temporary and remove it once the Phase 3.5/Phase 4 planning docs supersede it.
