# Phase 3 Open Items

Local verification status as of 2026-04-20:

- verified locally with a live multi-process setup:
  - real coordinator + 2 shards + 2 servers per shard
  - writes rejected before epoch start
  - coordinated `StartEpoch` opens writes across shards with matching epoch metadata
  - shard-local result files are written under a live coordinated epoch
  - deterministic boundary writes land in the expected shard and preserve payload bytes
  - a second coordinated epoch starts cleanly after the first completes
- still open:
  - hammer-load aggregate throughput improvement over the single-shard baseline
    - local measurement did not show an improvement yet (`~1233` accepted requests in the single-shard baseline epoch vs `~1076` total accepted requests across both shards in the routed 2-shard run)
    - AWS is not the next step until this local throughput gap is understood

Current rough edges / future work in the first Phase 3 cut:

- `completeEpoch()` only updates coordinator-local state; it does not verify that all shards actually completed
- coordinator `EpochStatus()` currently reports only coordinator-local shared state, not live fanout status from shard leaders
- `StartEpoch` fans out to shard leaders serially today; parallel fanout may be worth adding later if epoch-start latency starts to matter
- coordinator epoch start currently uses all-or-nothing fanout with best-effort rollback on partial failure; it is not a full 2PC/3PC-style durable commit protocol
- shard startup still has a timing race: a leader can begin accepting admin traffic before its peer RPC connection is fully established, so starting an epoch too soon after process launch can trip the merge path
- `Standby` pair config exists to prepare for future failover work, but coordinator routing currently uses only the active shard leader
- transport/auth still relies on the older certificate/index assumptions from the pre-coordinator architecture
- partial pair-delivery / rollback correctness is still deferred work; if one Riposte server in a shard pair receives a write and the other does not, that failure path is not yet fully hardened

Keep this list temporary and remove it once the full Phase 3 integration checks have been run.
