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

During an active epoch, the coordinator counts an upload only after `Upload3`
succeeds and the routed session is removed. Rejected uploads, failed uploads,
attempted client requests, and incomplete sessions are not counted. At epoch
completion, the active coordinator computes and caches a recommendation from
that completed epoch, then persists it to the control store.

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

When DynamoDB is the control store, recommendations are stored separately from
the authoritative shard topology:

```text
pk = scaling#epoch#<epoch_id>
pk = scaling#latest
pk = epoch-cycle
```

These records are proposals only. They do not update `pk="shard-config"`, do
not provision new shard machines, and do not change routing.

`pk="epoch-cycle"` records the inter-epoch milestone for operators and the
autoscaler. The normal path is:

```text
idle -> active -> recommendation_ready -> scaling_applied|scaling_skipped
```

`StartEpoch` is the gate for opening the next epoch. It accepts the first epoch
from `idle`, and later accepts only a settled previous outcome such as
`scaling_applied` or `scaling_skipped`. This keeps the durable state tied to
real milestones instead of advancing through synthetic workflow states.

`scaling_skipped` means the scaling decision is complete without changing
topology; it does not mean the epoch was skipped. The coordinator automatically
records `scaling_skipped` after persisting a `keep` recommendation because no
topology mutation is needed. For `grow` or `shrink`, an operator must either
apply the recommendation or explicitly skip it.

The manual apply path promotes a valid `scaling#latest` proposal into
`pk="shard-config"` only when no epoch is active or accepting:

```bash
coordinator -admin-target <addr> -dry-run-scaling-recommendation
coordinator -admin-target <addr> -apply-scaling-recommendation
coordinator -admin-target <addr> -skip-scaling-recommendation
```

Dry-run performs the same applicability checks and proposal build without
writing `pk="shard-config"`. Applying a recommendation changes the next
authoritative shard topology. Skipping a recommendation records the decision as
complete without changing topology. Neither path modifies historical
`shard-config#epoch#<epoch_id>` snapshots and it does not create new machines.
Extra `-shard` flags are treated as spare endpoint inventory; they are inactive
until a manually applied shard config includes them. Upload routing and epoch
start use the active `pk="shard-config"` record, not every endpoint listed on
the command line.

An AWS-side `autoscaler` process can run the same dry-run/apply sequence from
inside the eval environment:

```bash
autoscaler -coordinator <addr> -control-table <table> -once
autoscaler -coordinator <addr> -control-table <table> -once -apply
```

Without `-apply`, it only dry-runs and logs the decision. With `-apply`, it
promotes an applicable recommendation through the coordinator RPC. It does not
provision machines; shard endpoint inventory must already exist.

## Autoscaler And Topology Ownership

The autoscaler does not write `pk="shard-config"` directly. It reads DynamoDB to
decide whether work is worth attempting, then asks the coordinator to dry-run or
apply the recommendation through `ApplyScalingRecommendation`.

Current apply ownership is:

1. The autoscaler waits for `epoch-cycle = recommendation_ready`.
2. The autoscaler reads `scaling#latest`, `shard-config`, and `epoch`.
3. The coordinator has already auto-skipped `keep` recommendations, so
   `recommendation_ready` normally means `grow` or `shrink`.
4. The autoscaler calls coordinator `ApplyScalingRecommendation(DryRun=true)`.
5. If dry-run is applicable and `-apply` is set, the autoscaler records
   `scaling_in_progress` and calls
   `ApplyScalingRecommendation(DryRun=false)`.
6. The coordinator verifies it still holds the lease, repeats the safety checks,
   writes `pk="shard-config"`, and records `scaling_applied`.

An operator can also intentionally decline a `grow` or `shrink` proposal with
`SkipScalingRecommendation`, which records `scaling_skipped` and leaves
`pk="shard-config"` unchanged.

This duplication of checks is intentional. The autoscaler check is a cheap
preflight and log signal. The coordinator check is the authoritative guard
before mutation. If the epoch opens or the lease changes between autoscaler
preflight and apply, the coordinator rejects the write.

The alternative design is to let the autoscaler write `pk="shard-config"`
directly. That is more powerful, but it would make the autoscaler a second
topology writer. It would need the same lease/fencing checks, epoch-safety
checks, shard inventory validation, version conditions, and historical snapshot
rules as the coordinator. Until that ownership model is explicit, direct
autoscaler writes would increase split-brain and drift risk.

Therefore, the current rule is:

- coordinator owns epoch/control mutations and current topology writes
- autoscaler observes recommendations and requests dry-run/apply
- shard servers remain workers and never change topology

If future work makes the autoscaler the topology owner, first move all topology
mutation logic into a shared package or service with one writer policy. Do not
let coordinator and autoscaler independently implement different mutation
rules.

Local validation is available with:

```bash
script/phase3-local-apply-scaling.sh
```

That flow starts two shard pairs as endpoint inventory, seeds one active shard,
generates a real `grow` recommendation, applies it manually, and verifies the
next epoch routes global row `256` to shard 1.

A later timer-driven scheduler should sit above these milestones. It can start
an epoch for a configured duration, wait for the epoch timer to elapse, poll for
`recommendation_ready` with backoff, run the autoscaler, then start the next
epoch only after `scaling_applied` or `scaling_skipped`.

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

The coordinator exposes policy knobs for local/AWS experiments:

```text
-scaling-min-shards
-scaling-max-shards
-scaling-target-rows-per-shard
-scaling-up-density
-scaling-down-density
-scaling-max-shard-multiplier
```

By default, min/max shards both equal the current active shard count from the
control store, so the policy remains budget-safe unless a larger cap is
explicitly configured.

## Current Boundary

Existing per-shard table dimensions are compile-time constants and runtime local
table resizing is not implemented. Persisted recommendations can now be applied
manually between epochs, but automatic provisioning and automatic topology
changes remain future work.
