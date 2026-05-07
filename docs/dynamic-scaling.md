# Dynamic Shard Scaling

Phase 6 should recommend the next epoch's shard count from prior-epoch load.
It must not change shard count mid-epoch.

## Scaling Signal

For fresh-random hammer traffic, rows should route uniformly by the mathematics
of uniform random sampling. A single hot shard is therefore not the primary
scaling signal; it is more likely to be short-epoch noise, a small sample, or a
routing/RNG bug.

The first scaling policy uses accepted-request density:

```text
current_logical_rows = current_shard_count * target_rows_per_shard
request_density = accepted_requests / current_logical_rows
```

`target_rows_per_shard` defaults to `db.TABLE_HEIGHT`, currently `256`. The
logical dataset size is explicit in the coordinator shard map: each shard
contributes one full local table of rows, so `global_table_height =
shard_count * db.TABLE_HEIGHT`.

The current coordinator metrics are local and in-memory. During an active epoch,
the coordinator counts an upload only after `Upload3` succeeds and the routed
session is removed. Rejected uploads, failed uploads, attempted client requests,
and incomplete sessions are not counted. At epoch completion, the coordinator
computes and caches a recommendation from that completed epoch. Until metrics
are persisted later, a coordinator restart loses this scaling history.

Verification scripts capture the coordinator's status JSON after completed
sharded epochs. The scaling fields to inspect are:

- `scaling_epoch_id`
- `scaling_accepted_requests`
- `scaling_duration_secs`
- `request_density`
- `scaling_action`
- `scaling_reason`

AWS benchmark collection includes these fields in `comparison-summary.md` when
`status-completed-coordinator.json` is available for the sharded measured phase.

## Initial Policy Shape

Use hysteresis so shard count does not flap between epochs:

```text
grow if request_density >= 4.0
shrink if request_density <= 1.0
otherwise keep
```

Growth and shrink are bounded by a multiplier. With the default multiplier of
`2`, growth doubles shard count and shrink halves shard count before min/max
shard bounds are applied.

Examples:

```text
2 shards, 256 rows/shard, 1200 accepted:
  density = 1200 / (2 * 256) = 2.34
  keep 2 shards

2 shards, 256 rows/shard, 2500 accepted, max_shards=4:
  density = 2500 / (2 * 256) = 4.88
  grow to 4 shards

4 shards, 256 rows/shard, 5000 accepted, max_shards=4:
  density = 5000 / (4 * 256) = 4.88
  keep 4 shards because the shard budget cap is reached

4 shards, 256 rows/shard, 700 accepted, min_shards=1:
  density = 700 / (4 * 256) = 0.68
  shrink to 2 shards
```

## Budget Guardrail

`max_shards` is the budget cap. The policy should not recommend more shards
than this value. Table-row minimums and maximums are intentionally not separate
policy knobs in the first scaffold because logical rows are derived from shard
count:

```text
logical_rows = shard_count * target_rows_per_shard
```

## Current Boundary

This document is scaffolding only. Existing per-shard table dimensions are
compile-time constants, runtime local table resizing is not implemented, and
recommendations are not applied to the coordinator shard map yet. Applying a
new shard count at epoch boundaries and recording metrics durably remain future
work.
