# Testing Report

Date: 2026-05-08

This report currently records local preflight results and one previously
recorded AWS throughput result. For the final requirement, local-only results are
not enough: each reported claim should be backed by AWS deployment artifacts,
CloudWatch monitoring data, or measured AWS benchmark output.

Evidence standard: every result below is supported by one of:

- measured command output from this worktree
- generated local artifact paths from this worktree
- previously recorded AWS deployment result data already documented in this repo

Design claims are not treated as results unless backed by observed output,
CloudWatch data, or an artifact path.

## AWS Evidence Required For Final Submission

Use the local checks below as preflight only. The final report should replace or
supplement them with AWS evidence from one CloudWatch-enabled deployment:

```bash
CLOUDWATCH_OBSERVABILITY=1 \
CLOUDWATCH_LOG_RETENTION_DAYS=7 \
CONTROL_STORE_BACKEND=dynamodb \
SESSION_STORE_BACKEND=dynamodb \
INGESTION_QUEUE_BACKEND=sqs \
COMPLETED_UPLOAD_LEDGER_BACKEND=dynamodb \
HOT_STANDBY_INGESTION=1 \
PUBLIC_ENTRY_BACKEND=nlb \
PUBLIC_ENTRY_MULTI_COORDINATOR=1 \
./aws-eval/01-launch.sh

./aws-eval/02-deploy.sh
./aws-eval/03-smoke.sh
./aws-eval/10-validate-scaling-apply.sh
./aws-eval/13-demo-coordinator-failover-cloudwatch.sh
./aws-eval/14-demo-shard-auto-promotion-cloudwatch.sh
./aws-eval/15-demo-sqs-idempotence-cloudwatch.sh
./aws-eval/04-run-benchmark.sh
RESULT_ID=<run-id> ./aws-eval/05-collect-logs.sh
./aws-eval/06-teardown.sh
```

Minimum AWS evidence to capture:

| Category | AWS command/script | Evidence to attach |
| --- | --- | --- |
| Functional | `03-smoke.sh` | smoke result artifacts, coordinator/shard status JSON, result files for rows `0` and `256`, CloudWatch dashboard screenshot showing smoke status/events |
| Scaling | `10-validate-scaling-apply.sh` | scaling apply status JSON, DynamoDB `shard-config` before/after, epoch snapshots, CloudWatch screenshot around apply event |
| Failure | `13`, `14`, and `15` demo scripts | CloudWatch screenshots showing coordinator failover, shard auto-promotion, and SQS idempotence event markers plus collected JSON artifacts |
| Security | `01-launch.sh` and `05-collect-logs.sh` | Terraform output/state artifacts, security group/IAM artifacts, AWS console screenshots for SG ingress and instance profile/IAM policy |
| Performance | `04-run-benchmark.sh` and `05-collect-logs.sh` | `comparison-summary.md`, benchmark logs, CloudWatch dashboard screenshot covering measured phase |

For screenshots, use the CloudWatch dashboard URL printed by deploy/demo scripts:

```text
https://<region>.console.aws.amazon.com/cloudwatch/home?region=<region>#dashboards:name=<dashboard_name>
```

The final written result should cite the artifact paths and screenshot filenames
for each claim. If a script fails, report that category as failed and include the
failure artifact rather than describing the intended behavior.

## Summary

| ID | Category | Test case | Result | Evidence |
| --- | --- | --- | --- | --- |
| T1 | Functional | AWS sharded epoch lifecycle, deterministic routing, and durable ingestion | PASS | AWS smoke artifacts, CloudWatch dashboard/log group, DynamoDB/SQS/S3 artifacts |
| T2 | Scaling | Manual scaling recommendation apply from 1 to 2 shards | FAIL | Fresh local script output and `/tmp` artifacts |
| T3 | Failure | Coordinator/shard failure handling and retry/idempotence unit tests | PASS | Fresh `go test ./...` output |
| T4 | Security | AWS harness syntax and Terraform static validation | PASS | Fresh shell/Terraform command output |
| T5 | Performance | Short local baseline-vs-sharded throughput smoke | PASS | Fresh benchmark summary artifact |
| T6 | Performance | Long AWS baseline-vs-sharded throughput benchmark | PASS, prior evidence | Recorded AWS measurement in `docs/phase3-local-verification.md` |

## T1 Functional: AWS Sharded Epoch Lifecycle

Objective: measure whether the deployed AWS topology starts a coordinated epoch
across two shards, routes deterministic boundary-row writes through the public
NLB/coordinator path, persists completed-upload ingestion through SQS/S3 plus the
DynamoDB ledger, drains active and standby ingestion queues, and publishes
global-row result files.

Command:

```bash
FORCE=1 \
COORDINATOR_INSTANCE_TYPE=t3.small \
SERVER_INSTANCE_TYPE=t3.small \
CLIENT_INSTANCE_TYPE=t3.small \
CLIENT_CONCURRENCY=4 \
CLIENT_RETRY_OVERLOAD=1 \
WARMUP_EPOCH_SECONDS=10 \
MEASURED_EPOCH_SECONDS=30 \
POST_EPOCH_FLUSH_SECONDS=8 \
CONTROL_STORE_BACKEND=dynamodb \
SESSION_STORE_BACKEND=dynamodb \
INGESTION_QUEUE_BACKEND=sqs \
COMPLETED_UPLOAD_LEDGER_BACKEND=dynamodb \
HOT_STANDBY_INGESTION=1 \
PUBLIC_ENTRY_BACKEND=nlb \
PUBLIC_ENTRY_MULTI_COORDINATOR=0 \
CLOUDWATCH_OBSERVABILITY=1 \
CLOUDWATCH_LOG_RETENTION_DAYS=7 \
./aws-eval/01-launch.sh

./aws-eval/02-deploy.sh
FORCE_CONTROL_STATE=1 FORCE_SHARD_CONFIG=1 ./aws-eval/07-create-control-table.sh
./aws-eval/03-smoke.sh
RESULT_ID=functional-aws-20260509T024627Z ./aws-eval/05-collect-logs.sh
```

Expected:

- deployment uses `t3.small` EC2 instances with CloudWatch enabled
- coordinator and shard leaders report matching active/completed epoch metadata
- row `0` routes to shard `0`
- row `256` routes to shard `1`
- result files preserve global row coordinates
- active and standby SQS queues drain to visible `0` / in-flight `0`
- DynamoDB completed-upload ledger contains committed records for active and
  standby replicas
- CloudWatch log group receives coordinator/shard JSON status artifacts

Result: PASS.

Measured output:

```text
AWS smoke test passed.
  epoch: 1
  shard0 result: /Users/kevwjin/.codex/worktrees/7da7/riposte/aws-eval/.state/smoke/epoch-000001-shard-0-server-0.json
  shard1 result: /Users/kevwjin/.codex/worktrees/7da7/riposte/aws-eval/.state/smoke/epoch-000001-shard-1-server-0.json
```

Evidence:

- CloudWatch dashboard:
  `https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=riposte-aws-eval-20260509T024627Z`
- CloudWatch log group: `/riposte/aws-eval/20260509T024627Z`
- Collected result directory:
  `aws-eval/results/functional-aws-20260509T024627Z`
- Result file row evidence:
  `aws-eval/results/functional-aws-20260509T024627Z/leader-results/shard0-leader-run-epoch-000001-shard-0-server-0.json`
  contains global row `0` with payload `shard0-boundary`;
  `aws-eval/results/functional-aws-20260509T024627Z/leader-results/shard1-leader-run-epoch-000001-shard-1-server-0.json`
  contains global row `256` with payload `shard1-boundary`.
- DynamoDB epoch/control evidence:
  `aws-eval/.state/smoke/dynamodb-completed/epoch.json` reports epoch `1`
  state `completed`, accepting `false`, and shard config snapshot
  `shard-config#epoch#1`.
- SQS evidence:
  `aws-eval/results/functional-aws-20260509T024627Z/aws/ingestion/shard0-queue-attributes.json`,
  `shard1-queue-attributes.json`, `shard0-standby-queue-attributes.json`, and
  `shard1-standby-queue-attributes.json` all report visible `0` and in-flight
  `0`.
- Ledger evidence:
  `aws-eval/results/functional-aws-20260509T024627Z/aws/ingestion/completed-upload-ledger.json`
  contains four committed records: active and standby replicas for shard `0`
  and shard `1`.
- CloudWatch evidence:
  filtering the log group for `standby_promotable` returned completed
  coordinator JSON with `state: completed`, `scaling_accepted_requests: 2`,
  `ingestion_ack_error_count: 0`, active/standby committed counts of `1` for
  both shards, and `standby_promotable: true`.

## T2 Scaling: Manual Recommendation Apply

Objective: measure whether a completed epoch can generate a `grow` recommendation,
dry-run it, apply it to the authoritative shard config, and start the next epoch
with the expanded topology.

Command:

```bash
bash script/phase3-local-apply-scaling.sh
```

Expected:

- initial topology has one active shard and one spare configured shard
- row `256` is rejected before scaling apply
- epoch `1` persists a `grow` recommendation from `1` to `2` shards
- dry-run leaves the active config unchanged
- apply changes shard config version `1 -> 2`
- epoch `2` starts and routes row `256` to shard `1`

Result: FAIL.

The first half passed:

```text
PASS: initial active shard config uses one shard while two endpoints are configured
PASS: row 256 is rejected before scaling apply
PASS: epoch 1 persisted an applicable grow recommendation
PASS: dry-run validates the proposal without changing the active shard config
PASS: manual apply updated the active shard config to two shards
```

The failure occurred when starting epoch `2`:

```text
ERROR: could not parse epoch 2 id from: 2026/05/08 17:28:43 Could not start epoch: missing shard client for shard 1
```

Evidence:

- `/tmp/riposte-phase3-local/tmp/apply-scaling/apply-output.log` reported
  `applied=true`, `version=1->2`, `shards=1->2`,
  and `global_table_height=256->512`.
- `/tmp/riposte-phase3-local/tmp/apply-scaling/status-after-apply.json`
  reported `current_shard_count=2`, `global_table_height=512`, and
  `epoch_cycle_state=scaling_applied`.

Evidence-backed conclusion: the measured run proves the recommendation was
applied to status/control state, but it also proves the end-to-end scaling path
is currently blocked by a missing shard `1` client at the next epoch start.

## T3 Failure: Retry, Failover, And Idempotence Coverage

Objective: measure failure-handling code paths at the unit level, including
coordinator lease behavior, standby promotion, shard session loss, SQS
redelivery, and completed-upload idempotence.

Command:

```bash
go test ./...
```

Expected checks represented by unit tests:

- passive coordinator can become active after lease expiry
- stale/non-authoritative coordinators cannot mutate control state
- clients retry `Coordinator not active` and `Shard session lost` in coordinator mode
- SQS redelivery after ack failure can be handled with duplicate-skip ledger state
- active/standby ledger keys are separated by replica
- automatic promotion only happens when standby is caught up and active fails

Result: PASS.

Observed package results:

```text
ok   bitbucket.org/henrycg/riposte/autoscaler
ok   bitbucket.org/henrycg/riposte/client
ok   bitbucket.org/henrycg/riposte/controlstore
ok   bitbucket.org/henrycg/riposte/coordinator
ok   bitbucket.org/henrycg/riposte/db
ok   bitbucket.org/henrycg/riposte/mulproof
ok   bitbucket.org/henrycg/riposte/prf
ok   bitbucket.org/henrycg/riposte/utils
```

Measured evidence: `go test ./...` passed. Representative executed tests
include:

- `TestPassiveCoordinatorBecomesActiveAfterLeaseExpiry`
- `TestAutoPromotionWaitsForConsecutiveFailuresThenPromotes`
- `TestUploadShardBogusUUIDMapsToShardSessionLost`
- `TestUploadWithCoordinatorRetryRetriesShardSessionLostFromUpload1`
- `TestSQSCompletedUploadQueueFanoutWritesOnePayloadAndTwoPointers`
- `TestIngestionWorkerSkipsAlreadyCommittedDuplicate`
- `TestDemoFailIngestionAckOnceLeavesMessageForDuplicateRedelivery`

## T4 Security: AWS Harness And Terraform Static Validation

Objective: measure whether AWS harness scripts parse and Terraform configuration
validates locally before deployment.

Commands:

```bash
bash -n aws-eval/*.sh script/phase3-local-*.sh
terraform -chdir=aws-eval/terraform init -backend=false -input=false
terraform -chdir=aws-eval/terraform fmt -check
terraform -chdir=aws-eval/terraform validate
```

Expected:

- shell scripts parse cleanly
- Terraform initializes from the lock file
- Terraform formatting check passes
- Terraform validation passes
- Terraform can statically validate the AWS configuration before launch

Result: PASS.

Observed:

```text
Terraform has been successfully initialized!
Success! The configuration is valid.
```

Notes:

- The first `terraform validate` attempt failed because provider
  `hashicorp/aws v6.44.0` was not cached locally. After
  `terraform init -backend=false -input=false`, validation passed.
- This is a static measured result. It does not prove runtime IAM permissions,
  NLB exposure, or teardown behavior; those require AWS deployment evidence.

## T5 Performance: Short Local Throughput Smoke

Objective: run a short local throughput comparison to measure whether the sharded path can
complete a benchmark epoch and expose scaling status fields.

Command:

```bash
RIPOSTE_BENCH_SERVER_THREADS=2 \
RIPOSTE_BENCH_DURATION=4 \
RIPOSTE_BENCH_WARMUP_DURATION=0 \
RIPOSTE_BENCH_CLIENT_CONCURRENCY=4 \
bash script/phase3-local-benchmark.sh
```

Expected:

- baseline and sharded measured phases complete
- benchmark summary is written
- sharded coordinator status includes scaling fields

Result: PASS.

Observed summary:

| server_threads | client_concurrency | baseline_total | baseline_req_per_sec | shard0_total | shard1_total | sharded_total | sharded_req_per_sec | scaling_accepted_requests | request_density | scaling_action | delta | winner |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 4 | 400 | 100.00 | 239 | 244 | 483 | 120.75 | 480 | 0.9375 | keep | 83 | sharded |

Artifacts:

- `/tmp/riposte-phase3-local/benchmark-summary.tsv`
- `/tmp/riposte-phase3-local/benchmark-summary.md`

## T6 Performance: Long AWS Throughput Benchmark

Objective: use previously recorded AWS deployment evidence to report the
baseline-vs-sharded throughput result on separate EC2 hosts.

Command shape:

```bash
./aws-eval/00-preflight.sh
./aws-eval/01-launch.sh
./aws-eval/02-deploy.sh
./aws-eval/04-run-benchmark.sh
./aws-eval/05-collect-logs.sh
./aws-eval/06-teardown.sh
```

Expected:

- baseline measured epoch completes
- sharded measured epoch completes
- sharded accepted-request total exceeds baseline
- logs and comparison summary are collected

Result: PASS, prior AWS deployment evidence. This was not rerun in this worktree
session.

Recorded AWS measured result from `docs/phase3-local-verification.md`:

| measured_epoch_seconds | client_concurrency | retry_overload | baseline_total | baseline_req_per_sec | sharded_total | sharded_req_per_sec | delta | winner |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 600 | 16 | true | 38908 | 64.85 | 79024 | 131.71 | 40116 | sharded |

Evidence-backed conclusion: in that recorded AWS run, the 2-shard topology
accepted `79024` requests versus `38908` for the single-shard baseline.

## Current Risks And Follow-Up

- Scaling apply is not end-to-end passing in this worktree. The immediate bug is
  the missing shard `1` client after applying a `1 -> 2` shard config.
- Terraform validation is green after local provider initialization, but runtime
  AWS security validation still requires smoke/deploy evidence.
- The current performance smoke is intentionally short. Keep the long AWS
  benchmark as the authoritative throughput result.
- Failure handling has passing local unit-test evidence, but the three demo paths
  should still be rerun on AWS before presentation if the report will claim
  deployment-level durability: coordinator failover, shard auto-promotion, and
  SQS idempotence.
