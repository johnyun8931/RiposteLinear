# Phase 3 Open Items

These items are implemented in unit/focused tests but are not yet verified end to end with a live multi-process setup:

- real multi-process coordinator + multi-shard local run
- shard-local result files under a live coordinated epoch
- hammer-load aggregate throughput improvement over the single-shard baseline

Current rough edges / future work in the first Phase 3 cut:

- `completeEpoch()` only updates coordinator-local state; it does not verify that all shards actually completed
- coordinator `EpochStatus()` currently reports only coordinator-local shared state, not live fanout status from shard leaders
- `StartEpoch` fans out to shard leaders serially today; parallel fanout may be worth adding later if epoch-start latency starts to matter
- `Standby` pair config exists to prepare for future failover work, but coordinator routing currently uses only the active shard leader
- transport/auth still relies on the older certificate/index assumptions from the pre-coordinator architecture
- partial pair-delivery / rollback correctness is still deferred work; if one Riposte server in a shard pair receives a write and the other does not, that failure path is not yet fully hardened

Keep this list temporary and remove it once the full Phase 3 integration checks have been run.
