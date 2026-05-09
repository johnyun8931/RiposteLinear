# Riposte: Build, Test, and AWS Demo Guide

This document explains how to run the main correctness tests, local validation scripts, and AWS failure-handling demos for the Riposte project.

## Prerequisites

Install:

- Go 1.23+ or the version in `go.mod`
- Bash
- AWS CLI v2
- Terraform
- `jq`
- `python3`
- SSH client
- Optional: `uv`, for plotting scripts under `aws-eval/plotting`

AWS demos require an AWS account with permissions for:

- EC2
- IAM
- DynamoDB
- SQS
- S3
- Elastic Load Balancing
- CloudWatch Logs/Dashboards

Configure AWS credentials:

```bash
aws configure
```

Recommended default region:

```bash
us-east-1
```

## Run Unit Tests

From the repository root:

```bash
go test ./...
```

This runs Go unit tests for the client, coordinator, control store, database logic, ingestion queues, and server-related packages.

## Run Script and Terraform Checks

```bash
bash -n aws-eval/*.sh script/phase3-local-*.sh
terraform -chdir=aws-eval/terraform fmt -check
terraform -chdir=aws-eval/terraform validate
git diff --check
```

These checks verify shell syntax, Terraform formatting/validity, and whitespace errors.

## Run Local Functional Verification

```bash
script/phase3-local-verify.sh
```

This starts a local multi-shard topology, runs deterministic writes through the coordinator, verifies epoch completion, and checks result files.

## Run Local Scaling Apply Validation

```bash
script/phase3-local-apply-scaling.sh
```

This validates the manual scaling-apply path locally:

- starts with one active shard and spare shard inventory
- generates a grow recommendation
- applies it manually
- verifies the next epoch can route to the newly active shard range

## AWS Demo Setup

The AWS harness lives in:

```bash
aws-eval/
```

Most demos use this environment:

```bash
export AWS_REGION=us-east-1
export CONTROL_STORE_BACKEND=dynamodb
export SESSION_STORE_BACKEND=dynamodb
export PUBLIC_ENTRY_BACKEND=nlb
export INGESTION_QUEUE_BACKEND=sqs
export COMPLETED_UPLOAD_LEDGER_BACKEND=dynamodb
export HOT_STANDBY_INGESTION=1
export CLOUDWATCH_OBSERVABILITY=1

# Small instance types avoid quota issues.
export COORDINATOR_INSTANCE_TYPE=t3.small
export SERVER_INSTANCE_TYPE=t3.small
export CLIENT_INSTANCE_TYPE=t3.small
```

Launch infrastructure:

```bash
FORCE=1 RUN_ID=demo-$(date -u +%H%M%S) ./aws-eval/01-launch.sh
```

Deploy binaries and CloudWatch agent:

```bash
./aws-eval/02-deploy.sh
```

## AWS Read Server Feature

This branch also includes a read-serving path for the published Riposte table.
After each epoch merge, shard leaders can publish the latest merged shard table
to S3. Read servers are stateless: they load the current shard table artifacts
from S3 into memory, expose an HTTP API, and sit behind a public Application Load
Balancer.

The read server API includes:

- `GET /healthz`: readiness check used by the ALB target group
- `GET /status`: current loaded epochs, shard ranges, server ID, and refresh
  state
- `GET /read?x=<column>&y=<global_row>`: returns the message at the requested
  coordinate

The AWS launch/deploy flow creates and configures the read-side pieces:

- read table S3 bucket
- readserver launch template
- readserver Auto Scaling Group
- public read ALB
- ALB target group using `/healthz`
- `readserver` binary uploaded to S3 for read instances to download on boot
- `readload` binary copied to the client instance for read simulations

The read path is separate from the write path. Writes still go through the
coordinator/shard path; reads go through the ALB to stateless read servers.

## Simulate Reads Through the Read ALB

After launch and deploy, first run a smoke or failover/read script that publishes
a deterministic table. Then use the read scripts below.

Run read-server failover while sustained reads are flowing:

```bash
./aws-eval/12-validate-read-failover.sh
```

This script publishes a deterministic table, starts background read load through
the ALB, terminates one read server, verifies reads continue through the
remaining target, waits for ASG replacement, and saves CloudWatch graphs under
`aws-eval/results/read-failover-<timestamp>/`.

Run a flat read-load simulation:

```bash
READ_LOAD_DURATION_SECONDS=600 \
READ_LOAD_CONCURRENCY=128 \
./aws-eval/13-run-read-load.sh
```

Run a staged read-load profile with baseline, spike, and cooldown phases:

```bash
./aws-eval/14-run-read-load-profile.sh
```

Read simulation outputs are saved under:

```bash
aws-eval/results/read-load-<timestamp>/
aws-eval/results/read-load-profile-<timestamp>/
aws-eval/results/read-failover-<timestamp>/
```

The useful artifacts are the JSON summaries, ALB target-health snapshots, and
CloudWatch graph PNGs for request count, HTTP response codes, target response
time, healthy host count, and connection errors.

## AWS Failure Demo 1: Mid-Epoch Coordinator Failover

```bash
./aws-eval/13-demo-coordinator-mid-epoch-failover-cloudwatch.sh
```

Expected success output includes:

```text
AWS mid-epoch coordinator failover validation passed.
passive-route payload: strict-passive-route
post-lease-acquisition payload: strict-active-route
```

This demonstrates:

- coordinator B routes a write while passive
- coordinator A dies during epoch 1
- coordinator B acquires the lease during epoch 1
- writes continue through B

CloudWatch evidence query:

```sql
fields @timestamp, event, detail
| filter scenario = "coordinator-mid-epoch-failover" and role = "demo-event"
| sort @timestamp asc
```

Look for:

- `passive_route_succeeded`
- `kill_coordinator_a_mid_epoch`
- `coordinator_b_active_mid_epoch`
- `active_route_succeeded`
- `scenario_complete`

## AWS Failure Demo 2: Automatic Shard Standby Promotion

```bash
./aws-eval/14-demo-shard-auto-promotion-cloudwatch.sh
```

Expected success output includes:

```text
AWS automatic hot standby promotion validation passed.
```

This demonstrates:

- active shard 0 leader is killed
- coordinator detects active shard failure
- standby shard is promoted automatically
- next write reaches the promoted standby

CloudWatch evidence query:

```sql
fields @timestamp, event, detail
| filter scenario = "shard-auto-promotion" and role = "demo-event"
| sort @timestamp asc
```

Look for:

- `scenario_start`
- `kill_shard0_active`
- `shard0_auto_promoted`
- `scenario_complete`

## AWS Failure Demo 3: SQS Redelivery and Idempotence

```bash
./aws-eval/15-demo-sqs-idempotence-cloudwatch.sh
```

Expected success output includes:

```text
AWS SQS idempotence CloudWatch demo passed.
```

This demonstrates:

- the shard worker commits a completed upload
- the first SQS ack/delete is intentionally failed
- SQS redelivers the message
- the completed-upload ledger detects the duplicate
- the duplicate is skipped and the message is eventually acked

CloudWatch event query:

```sql
fields @timestamp, event, detail
| filter scenario = "sqs-idempotence" and role = "demo-event"
| sort @timestamp asc
```

Look for:

- `upload_start`
- `queue_drained`
- `scenario_complete`

CloudWatch counter query:

```sql
fields @timestamp, target,
  status.ingestion_ack_error_count,
  status.completed_upload_duplicate_skip_count,
  status.ingestion_ack_count,
  status.ingestion_queue_depth,
  status.ingestion_inflight_count
| filter scenario = "sqs-idempotence" and role = "shard-status"
| sort @timestamp desc
```

Expected proof row for `shard0-active`:

- `ingestion_ack_error_count = 1`
- `completed_upload_duplicate_skip_count = 1`
- `ingestion_ack_count = 1`
- `ingestion_queue_depth = 0`
- `ingestion_inflight_count = 0`

## Collect AWS Logs and Artifacts

After any AWS demo:

```bash
source aws-eval/.state/env.sh
RESULT_ID=${RUN_ID}-demo ./aws-eval/05-collect-logs.sh
```

Collected results are written under:

```bash
aws-eval/results/
```

## Teardown AWS Resources

When finished:

```bash
./aws-eval/06-teardown.sh
```

Then verify no tagged EC2 instances remain:

```bash
aws ec2 describe-instances \
  --region us-east-1 \
  --filters \
    'Name=tag:Project,Values=riposte-aws-eval' \
    'Name=instance-state-name,Values=pending,running,stopping,stopped' \
  --query 'Reservations[].Instances[].{InstanceId:InstanceId,State:State.Name,Name:Tags[?Key==`Name`]|[0].Value,RunId:Tags[?Key==`RunId`]|[0].Value}' \
  --output table
```

Note: teardown removes the CloudWatch dashboard/log group for that run. Take screenshots first if the dashboard is needed for the report.

## Plotting / Report Figures

Plotting scripts are under:

```bash
aws-eval/plotting/
```

The report source and generated figures are under:

```bash
report/
```

The main LaTeX file is:

```bash
report/main.tex
```

A prebuilt PDF is:

```bash
report/main.pdf
```

## Common Issues

### EC2 quota errors

Use smaller instances:

```bash
export COORDINATOR_INSTANCE_TYPE=t3.small
export SERVER_INSTANCE_TYPE=t3.small
export CLIENT_INSTANCE_TYPE=t3.small
```

### Stale AWS state

If a previous run failed, tear down first:

```bash
./aws-eval/06-teardown.sh
```

Then relaunch with:

```bash
FORCE=1 ./aws-eval/01-launch.sh
```

### CloudWatch dashboard missing

The dashboard is destroyed during teardown. Use local screenshots or collected artifacts under `aws-eval/results/`.

## Recommended Full Validation Sequence

```bash
go test ./...
bash -n aws-eval/*.sh script/phase3-local-*.sh
terraform -chdir=aws-eval/terraform fmt -check
terraform -chdir=aws-eval/terraform validate
git diff --check

script/phase3-local-verify.sh
script/phase3-local-apply-scaling.sh
```

For AWS demos, run one failure demo at a time. Do not run multiple AWS eval scripts concurrently against the same `.state` directory.
