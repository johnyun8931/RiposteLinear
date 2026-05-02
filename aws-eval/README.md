# Riposte AWS Evaluation

This directory contains a first-pass AWS CLI + SSH evaluation harness for the
current sharded coordinator topology.

It is intentionally operational, not permanent infrastructure:

- no Terraform
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
- client hammer concurrency remains the built-in `16`

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

- AWS CLI v2 authenticated with EC2 and SSM AMI-lookup permissions
- `go`, `ssh`, `scp`, `curl`, `python3`, `file`
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

Creates:

- one temporary AWS key pair
- one security group
  - SSH from current public IP only
  - all TCP within the security group
- six tagged EC2 instances

State is written to:

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

### 4. Smoke

```bash
./aws-eval/03-smoke.sh
```

Starts the full sharded topology, retries coordinator `StartEpoch` until the
leaders are ready, sends deterministic writes for row `0` and row `128`,
waits for completion, and verifies:

- each shard leader wrote its own result file
- the payload bytes landed in the expected shard

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
- `comparison-summary.md`
- raw logs per node
- copied result JSON files under `leader-results/`

### 7. Teardown

Run teardown even if earlier steps fail:

```bash
./aws-eval/06-teardown.sh
```

This terminates the six instances, deletes the temporary key pair, and deletes
the security group after attachments drain.

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
WARMUP_EPOCH_SECONDS=60
MEASURED_EPOCH_SECONDS=600
START_EPOCH_RETRY_TIMEOUT=90
START_EPOCH_RETRY_INTERVAL=2
POST_EPOCH_FLUSH_SECONDS=12
CLIENT_EXIT_GRACE_SECONDS=30
```

Use explicit subnet/AZ overrides only when the default-network auto-selection
does not find capacity in the chosen region.
