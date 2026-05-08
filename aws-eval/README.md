# Riposte AWS Evaluation

This directory contains a Terraform + AWS CLI + SSH evaluation harness for the
current sharded coordinator topology.

It is intentionally operational, not permanent infrastructure:

- no ALB / HTTPS proxy layer
- no active-passive or checkpoint work
- no health-check protocol changes

The goal is to answer two questions on real EC2 hosts:

1. does the coordinator + 2-shard topology run correctly across machines?
2. does the routed 2-shard path beat the single-shard baseline under the same
   client shape?

## Topology

The harness launches six EC2 instances in one subnet / one AZ:

- `1` coordinator
- `4` shard servers
  - shard 0 leader
  - shard 0 follower
  - shard 1 leader
  - shard 1 follower
- `1` client/load-generator

Application traffic stays on private IPs. Public IPs are used only for SSH and
`scp`.

Default instance types:

- coordinator: `c7i.large`
- shard nodes: `c7i.large`
- client: `c7i.xlarge`

Default region:

- `us-east-1`

## Benchmark Shape

The initial cloud configuration reuses the local winner:

- `server -threads 2`
- `client -threads 1`
- client hammer concurrency defaults to `16`, and can be overridden with
  `CLIENT_CONCURRENCY`
- overload retry is opt-in with `CLIENT_RETRY_OVERLOAD=1`; when enabled, the
  client treats ready-queue overload as backpressure and retries the same
  plaintext with exponential backoff
- random hammer uploads generate a fresh random message per request; deterministic
  `-x` / `-y` / `-payload` uploads intentionally reuse the exact message

Measured phases:

- baseline:
  - start shard 0 pair only
  - `60s` warm-up epoch
  - `600s` measured epoch
  - client targets shard 0 leader directly with `-leader`
- sharded:
  - start coordinator + both shard pairs
  - `60s` warm-up epoch
  - `600s` measured epoch
  - client targets the coordinator with `-coordinator`

Primary comparison metric:

- baseline total = shard 0 leader accepted requests during measured epoch
- sharded total = shard 0 leader + shard 1 leader accepted requests during
  measured epoch

The parser also preserves 10-second req/sec samples from leader logs.

Benchmark validity is separate from throughput. A valid phase should end when
the epoch closes and the hammer client exits after receiving `No active epoch`.
Process killing is cleanup between phases, not a successful completion signal.
If the client exits with `unexpected EOF`, times out, or accepted traffic stops
too early in the measured epoch, the summary reports `winner: unavailable` and
keeps the raw totals as diagnostic data.
If overload retry is off and the server returns `server overloaded: ready queue
full`, the phase is marked as `overload`; that is diagnostic data, not a valid
throughput comparison. If overload retry is on, overload lines are counted as
retry diagnostics and the phase can still be valid if the client exits via
`No active epoch`.

The benchmark stops at the first invalid phase. For example, if
`baseline-warmup` fails validation, the measured phases are skipped because the
baseline lifecycle is already unhealthy.

## Generated Local Files

Generated files are git-ignored:

- `aws-eval/.state/`
- `aws-eval/keys/`
- `aws-eval/bin/`
- `aws-eval/results/`

## Prerequisites

- AWS CLI v2 authenticated with EC2, IAM, ELBv2, DynamoDB, and SSM AMI-lookup
  permissions
- `go`, `ssh`, `scp`, `curl`, `python3`, `file`, `terraform`
- enough AWS quota for six On-Demand instances in one AZ

## Workflow

### 1. Preflight

```bash
./aws-eval/00-preflight.sh
```

Checks:

- AWS identity and effective region
- selected default VPC / subnet / AZ
- `c7i.large` and `c7i.xlarge` availability in that AZ
- Ubuntu AMI lookup through SSM
- local `go test` for deployment-relevant packages
- Linux `amd64` cross-compilation for `server`, `client`, and `coordinator`

### 2. Launch

```bash
./aws-eval/01-launch.sh
```

Runs Terraform from `aws-eval/terraform/` and writes compatibility state to
`aws-eval/.state/env.sh` for the remaining scripts. Terraform creates:

- one temporary AWS key pair
- one security group
  - SSH from current public IP only
  - all TCP within the security group
- optionally, an internet-facing Network Load Balancer for public coordinator
  RPC ingress when `PUBLIC_ENTRY_BACKEND=nlb`
- six tagged EC2 instances
- optional DynamoDB control/session tables when they do not already exist
- optional coordinator IAM role/profile for DynamoDB runtime access

The wrapper preserves the old shell interface: set the same env vars as before,
then run `01-launch.sh`. Terraform variables are generated under
`aws-eval/.state/`, and Terraform state is local under `aws-eval/terraform/`.
State for downstream scripts is written to:

```text
aws-eval/.state/env.sh
```

### 3. Deploy

```bash
./aws-eval/02-deploy.sh
```

Builds Linux binaries locally and copies:

- `~/server` to each shard node
- `~/coordinator` to the coordinator node
- `~/client` to the client node

### Optional DynamoDB Tables

The coordinator defaults to in-memory control and session stores. To use
DynamoDB for opt-in control/session-store testing, launch with at least one
DynamoDB backend enabled so Terraform attaches a coordinator IAM instance
profile:

```bash
CONTROL_STORE_BACKEND=dynamodb SESSION_STORE_BACKEND=dynamodb ./aws-eval/01-launch.sh
```

Terraform creates missing DynamoDB tables during launch. If a table already
exists, Terraform leaves it unmanaged and the helper below records and seeds it:

```bash
./aws-eval/07-create-control-table.sh
```

The table has one string partition key, `pk`. It stores control records
`lease`, `epoch`, and `shard-config`. The `shard-config` record stores the full
two-shard topology used by the AWS harness: shard count, rows per shard, global
table height, and active shard addresses. Each started epoch also gets an
immutable `shard-config#epoch#<epoch_id>` topology snapshot, and the `epoch`
record points at it with `shard_config_key`. Completed epochs may also write
`scaling#epoch#<epoch_id>` and `scaling#latest` proposal records; operators can
dry-run, apply, or explicitly skip an applicable latest recommendation with
`coordinator -admin-target <addr> -dry-run-scaling-recommendation`,
`coordinator -admin-target <addr> -apply-scaling-recommendation`, or
`coordinator -admin-target <addr> -skip-scaling-recommendation` between epochs.
Extra `-shard` flags are spare endpoint inventory, not active topology, until a
new `shard-config` version includes them. When the session store uses the same
table, it also stores transient `session#<global_uuid>` records for
coordinator-routed uploads that have completed `Upload1` but not yet completed
`Upload3`. The helper records `CONTROL_STORE_BACKEND=dynamodb`,
`DYNAMODB_CONTROL_TABLE`, `DYNAMODB_CONTROL_REGION`,
`SESSION_STORE_BACKEND=dynamodb`, `DYNAMODB_SESSION_TABLE`, and
`DYNAMODB_SESSION_REGION` in `aws-eval/.state/env.sh`.

Terraform creates a temporary coordinator IAM role and instance profile for
DynamoDB runtime access. Teardown removes Terraform-managed role/profile
resources with the EC2 resources.

### Optional Public Entry NLB

The harness defaults to private client-to-coordinator traffic inside the eval
security group:

```bash
PUBLIC_ENTRY_BACKEND=none
```

To add a public entry layer, launch with:

```bash
PUBLIC_ENTRY_BACKEND=nlb ./aws-eval/01-launch.sh
```

This creates an internet-facing Network Load Balancer, a TCP target group for
the coordinator RPC port, and a listener on that same port. Smoke and sharded
benchmark client traffic use the NLB DNS name when enabled. Coordinator
administration and coordinator-to-shard traffic still use private addresses.

By default this registers one coordinator target. For multi-coordinator ingress
validation, launch with:

```bash
CONTROL_STORE_BACKEND=dynamodb SESSION_STORE_BACKEND=dynamodb \
PUBLIC_ENTRY_BACKEND=nlb PUBLIC_ENTRY_MULTI_COORDINATOR=1 \
./aws-eval/01-launch.sh
```

This registers the coordinator instance on both `COORDINATOR_PORT` and
`COORDINATOR_STANDBY_PORT`. The active coordinator still owns epoch/control
mutations through the DynamoDB lease, while both coordinator processes can route
uploads through the shared DynamoDB session store while the epoch is accepting.

### Optional Durable Ingestion

The harness defaults to the process-local memory completed-upload queue:

```bash
INGESTION_QUEUE_BACKEND=memory
```

To validate the durable completed-upload boundary, launch with:

```bash
INGESTION_QUEUE_BACKEND=sqs ./aws-eval/01-launch.sh
```

Terraform creates one SQS queue per shard and one S3 bucket for completed-upload
payloads. Shard leaders write the full completed-upload job to S3, enqueue a
small SQS pointer, and delete the SQS message only after prepare/commit
succeeds. S3 payloads are retained after ack for debugging and replay audit.

Smoke captures SQS queue attributes and S3 payload listings under
`smoke/run/logs/ingestion-completed/`. `05-collect-logs.sh` also copies current
ingestion artifacts under `aws/ingestion/`.

### 4. Smoke

```bash
./aws-eval/03-smoke.sh
```

Starts the full sharded topology, retries coordinator `StartEpoch` until the
leaders are ready, sends deterministic writes for global row `0` and global row
`256`,
waits for completion, and verifies:

- each shard leader wrote its own result file
- the payload bytes landed in the expected shard, with shard 1 publishing
  global row `256`
- completed coordinator status reports scaling metrics for the two accepted
  smoke writes and `global_table_height=512`

If `PUBLIC_ENTRY_BACKEND=nlb`, smoke waits for the coordinator target to become
healthy and sends deterministic client writes through the NLB DNS name. NLB
target-health artifacts are copied under the smoke log directory.

If `CONTROL_STORE_BACKEND=dynamodb`, smoke starts the coordinator with the
DynamoDB control-store flags and captures the `lease`, `epoch`, and
`shard-config` table records, plus the epoch-bound `epoch-shard-config.json`
snapshot, after epoch start and completion. If
`SESSION_STORE_BACKEND=dynamodb`, coordinator status reports that backend and
the coordinator persists in-flight session records in DynamoDB until `Upload3`
completes. The smoke check also verifies that the DynamoDB epoch ID and
accepting flag match the observed coordinator lifecycle, and that the persisted
current and epoch-bound shard configs report two shards, `rows_per_shard=256`,
and `global_table_height=512`.

If `INGESTION_QUEUE_BACKEND=sqs`, smoke waits for both shard queues to drain,
verifies the SQS approximate visible/in-flight counts are zero, and verifies
that at least two completed-upload payloads were written to S3.

To validate coordinator lease/fencing behavior with two coordinator attempts:

```bash
CONTROL_STORE_BACKEND=dynamodb ./aws-eval/08-validate-coordinator-lease.sh
```

This starts coordinator A, starts coordinator B in warm standby, verifies B
stays passive while A holds the lease, stops A, then verifies B promotes with a
newer fencing token and starts an epoch.

To validate multi-coordinator ingress through the NLB:

```bash
CONTROL_STORE_BACKEND=dynamodb SESSION_STORE_BACKEND=dynamodb \
PUBLIC_ENTRY_BACKEND=nlb PUBLIC_ENTRY_MULTI_COORDINATOR=1 \
./aws-eval/09-validate-multi-coordinator-ingress.sh
```

This starts coordinator A and standby coordinator B, forces NLB client traffic
to B while A still owns the lease, verifies B can route uploads through the
shared session store, then stops A and verifies B promotes and handles a new
epoch.

To validate manual scaling recommendation apply:

```bash
CONTROL_STORE_BACKEND=dynamodb SESSION_STORE_BACKEND=dynamodb \
./aws-eval/10-validate-scaling-apply.sh
```

This seeds `pk="shard-config"` with one active shard while keeping both shard
endpoints configured, generates a real `grow` recommendation, validates it with
`-dry-run-scaling-recommendation`, applies it with
`-apply-scaling-recommendation`, and verifies the next epoch routes global row
`256` to shard 1. Dry-run, apply, status, and DynamoDB artifacts are written under
`aws-eval/.state/scaling-apply/`.

To validate the in-cloud autoscaler-driven apply path instead of direct local
admin apply:

```bash
CONTROL_STORE_BACKEND=dynamodb SESSION_STORE_BACKEND=dynamodb \
APPLY_WITH_AUTOSCALER=1 ./aws-eval/10-validate-scaling-apply.sh
```

This runs `~/autoscaler -once -apply` on the coordinator EC2 instance. The
autoscaler still uses preconfigured spare shard inventory; it does not create or
terminate EC2 instances.

### 5. Benchmark

```bash
./aws-eval/04-run-benchmark.sh
```

Runs:

- `baseline-warmup`
- `baseline-measured`
- `sharded-warmup`
- `sharded-measured`

Each phase writes logs and result files under `/tmp/riposte-eval/` on the
remote instances. The benchmark waits for the hammer client to exit for up to
the epoch duration plus `CLIENT_EXIT_GRACE_SECONDS`. The grace period is
post-epoch slack for client shutdown and is not part of the measured throughput
window.

Sharded benchmark phases also capture coordinator status artifacts in the phase
log directory:

- `status-active-coordinator.json`
- `status-completed-coordinator.json`

The completed status includes `scaling_epoch_id`,
`scaling_accepted_requests`, `scaling_duration_secs`, `request_density`,
`scaling_action`, `scaling_reason`, `latest_scaling_epoch_id`,
`latest_scaling_action`, `latest_scaling_recommended_shards`,
`scaling_apply_status`, and `scaling_apply_reason`.

If `PUBLIC_ENTRY_BACKEND=nlb`, sharded benchmark hammer clients target the NLB
DNS name and each sharded phase captures NLB target-health snapshots.

For AWS load calibration, run the short concurrency sweep after smoke:

```bash
CLIENT_CONCURRENCY_SWEEP="1 2 4 8 16" \
CLIENT_RETRY_OVERLOAD=1 \
WARMUP_EPOCH_SECONDS=10 \
MEASURED_EPOCH_SECONDS=45 \
CLIENT_EXIT_GRACE_SECONDS=30 \
./aws-eval/04-run-concurrency-sweep.sh
```

The sweep runs the normal benchmark once per concurrency value, collects logs
after each attempt, and writes an aggregate summary under
`aws-eval/results/<sweep-id>/`.

### 6. Collect

```bash
./aws-eval/05-collect-logs.sh
```

Copies remote logs/results into:

```text
aws-eval/results/<timestamp>/
```

Outputs:

- `metadata.json`
- `baseline-throughput.csv`
- `sharded-throughput.csv`
- `comparison-summary.md`, including a scaling-status section when the
  sharded measured coordinator status artifact is present
- raw logs per node
- copied result JSON files under `leader-results/`

### 7. Teardown

Run teardown even if earlier steps fail:

```bash
./aws-eval/06-teardown.sh
```

This runs `terraform destroy` for the six instances, temporary key pair,
security group, IAM role/profile, NLB resources, and any DynamoDB tables
Terraform created for the run. Local state, keys, binaries, and copied results
remain in ignored paths for audit/debug.

## Useful Overrides

```bash
AWS_REGION=us-east-1
AWS_PROFILE=default
VPC_ID=vpc-...
SUBNET_ID=subnet-...
AVAILABILITY_ZONE=us-east-1a
COORDINATOR_INSTANCE_TYPE=c7i.large
SERVER_INSTANCE_TYPE=c7i.large
CLIENT_INSTANCE_TYPE=c7i.xlarge
SERVER_THREADS=2
CLIENT_THREADS=1
CLIENT_CONCURRENCY=16
CLIENT_RETRY_OVERLOAD=0
CLIENT_OVERLOAD_BACKOFF_INITIAL_MS=10
CLIENT_OVERLOAD_BACKOFF_MAX_MS=250
CONTROL_STORE_BACKEND=memory
DYNAMODB_CONTROL_TABLE=riposte-aws-eval-control
DYNAMODB_CONTROL_REGION=us-east-1
SESSION_STORE_BACKEND=memory
DYNAMODB_SESSION_TABLE=riposte-aws-eval-control
DYNAMODB_SESSION_REGION=us-east-1
COORDINATOR_HOLDER_ID=riposte-aws-eval-run-coordinator
WARMUP_EPOCH_SECONDS=60
MEASURED_EPOCH_SECONDS=600
START_EPOCH_RETRY_TIMEOUT=90
START_EPOCH_RETRY_INTERVAL=2
POST_EPOCH_FLUSH_SECONDS=12
CLIENT_EXIT_GRACE_SECONDS=30
PUBLIC_ENTRY_BACKEND=none
PUBLIC_ENTRY_MULTI_COORDINATOR=0
TF_VAR_vpc_id=vpc-...
TF_VAR_subnet_id=subnet-...
TF_VAR_ssh_cidr=203.0.113.10/32
TERRAFORM_AUTO_APPROVE=1
```

Use explicit subnet/AZ overrides only when the default-network auto-selection
does not find capacity in the chosen region.
