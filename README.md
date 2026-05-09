# WARNING!!!

DO NOT USE THIS SOFTWARE TO SECURE ANY SORT OF REAL-WORLD COMMUNICATIONS!

This software is for performance testing ONLY! It is full of security vulnerabilities that could be exploited in any real-world deployment.

The purpose of this software is to evaluate the performance of the Riposte system, NOT to be used in a deployment scenario.

# Riposte AWS Architecture Report README

This README summarizes the AWS architecture used for the project report and explains how to run the main correctness tests, local validation scripts, and AWS failure-handling demos. The executable AWS evaluation harness on this branch validates the write path, scaling path, and failure-handling path. The ALB-backed read path is partner-owned work that will be pushed separately.

## High-Level Deployment Description

This project deploys a dynamically sharded, epoch-based Riposte-style anonymous write relay on AWS. Clients submit writes through the write ingress path, where a Network Load Balancer forwards custom TCP/TLS RPC traffic to coordinator processes. Coordinators route each upload to the correct write shard using the current shard topology, persist shared session state in DynamoDB, and forward the existing `Upload1` / `Upload2` / `Upload3` protocol to shard leaders.

Each write shard is deployed as a leader/follower pair. After a shard accepts a completed `Upload3`, the shard writes the completed-upload payload to S3 and sends compact pointer messages through shard-specific SQS queues. Workers consume those messages, load the payload from S3, process the completed upload, and acknowledge the SQS message only after successful processing. Hot standby ingestion can send the same pointer to active and standby queues so a standby replica stays caught up for promotion.

Reads use a separate path in the report architecture. Published table artifacts are stored in S3, and stateless read servers load those artifacts into memory. Clients reach read servers through an Application Load Balancer using HTTP JSON requests. This split keeps the existing write protocol on NLB/custom RPC while using ALB/HTTP for stateless reads, semantic health checks, and cleaner CloudWatch metrics. The ALB/read tier is expected from the partner read-path branch; the `aws-eval/` scripts on this branch are focused on write-path AWS validation.

## Cloud Services Used

- **EC2:** Runs coordinator processes, shard leader/follower processes, workers, client/load generators, and autoscaler processes in the write-path AWS evaluation harness. The partner read-path work also uses EC2 read-server processes.
- **Network Load Balancer:** Provides Layer 4 ingress for the custom TCP/TLS coordinator write protocol.
- **Application Load Balancer:** Provides Layer 7 HTTP ingress for stateless read requests and `/healthz` checks in the partner read-path/report architecture.
- **DynamoDB:** Stores coordinator lease/fencing state, epoch metadata, shard topology, immutable epoch topology snapshots, coordinator sessions, completed-upload ledger records, scaling recommendations, and epoch-cycle state.
- **S3:** Stores completed-upload payloads after accepted `Upload3` and stores published table/result artifacts used by read servers.
- **SQS:** Carries shard-specific completed-upload pointer messages for durable post-`Upload3` processing and redelivery.
- **IAM:** Grants EC2 runtime roles scoped access to DynamoDB, S3, and SQS resources.
- **CloudWatch:** Stores logs and metrics used for evidence screenshots, failure validation, scaling validation, and throughput summaries.
- **Terraform:** Creates and tears down the repeatable AWS evaluation infrastructure used by the write-path demos.

## Scaling and Failure Demonstrations

Scaling was demonstrated at epoch boundaries. After an epoch completed, the coordinator computed accepted-request density and persisted a scaling recommendation. For validation, the scaling threshold was intentionally configured to make a short AWS run trigger a `grow` recommendation. The apply path then changed the authoritative shard configuration from one active shard to two active shards, increasing global table height from 256 to 512. The report evidence shows the recommendation, density, shard-count change, global-table-height change, and `scaling_applied` cycle state.

Failure behavior was demonstrated with bounded AWS scenarios:

- **Coordinator failover:** A coordinator holding the DynamoDB lease was stopped during an active epoch. A standby coordinator later acquired the lease using DynamoDB fencing while shared sessions allowed upload routing to continue through available coordinators.
- **Shard hot standby promotion:** The active shard leader was stopped. The coordinator detected the failure, promoted the hot standby shard replica, and a subsequent write reached the promoted standby.
- **SQS redelivery/idempotence:** The ingestion worker was forced to fail one SQS acknowledgement after processing. SQS redelivered the pointer, the worker detected the completed-upload ledger entry, skipped duplicate processing, and acknowledged the redelivered message.

The main remaining limitation is that partial shard-side `Upload1` / `Upload2` state is not fully durably replayed after shard death. The durable replay boundary begins after a shard accepts `Upload3` and writes the completed-upload payload to S3/SQS.

## Division of Work

- **Kevin Jin:** Implemented and evaluated the write path. Responsibilities included coordinator routing, sharded write topology, DynamoDB control/session state, coordinator lease/fencing, SQS/S3 completed-upload ingestion, completed-upload ledger behavior, scaling recommendation/apply flow, failure demonstrations, AWS write-path validation, and the main report integration.
- **John Yun:** Implemented and evaluated the read path for the report architecture. Responsibilities included moving published server data to S3, building the read-server path that loads S3-published table artifacts, exposing the HTTP JSON read API, configuring ALB-based read ingress and health checks, and validating that clients can read published data from the S3-backed read tier.

# Riposte: Build, Test, and AWS Demo Guide

This section explains how to run the main correctness tests, local validation scripts, and AWS failure-handling demos for the Riposte project.

## Prerequisites

Install:

- Go 1.24+ or the version in `go.mod`
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

## Build Local Binaries

From the repository root:

```bash
go build ./client ./server ./coordinator ./autoscaler
```

This verifies that the main executable packages build locally. The AWS deploy script builds and ships the binaries it needs during `./aws-eval/02-deploy.sh`, but this command is the simplest local compile check.

## Run Script and Terraform Checks

```bash
bash -n aws-eval/*.sh script/phase3-local-*.sh
terraform -chdir=aws-eval/terraform init -backend=false -input=false
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
export PUBLIC_ENTRY_MULTI_COORDINATOR=1
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


## Reads:
for the demo for reads please look into the [from-failover-add-read-servers](https://github.com/johnyun8931/RiposteLinear/tree/from-failover-add-read-servers) branch. 

### overview:
The [from-failover-add-read-servers](https://github.com/johnyun8931/RiposteLinear/tree/from-failover-add-read-servers) branch extends the AWS evaluation system with a new stateless read-serving path. The write path still uses the existing coordinator and shard-server flow, but after each epoch merge, shard leaders can publish the merged table artifact to S3. Read servers then load the latest S3-published table into memory and serve client reads through an HTTP API.

This branch adds:

- S3 publication of merged shard tables after epoch completion
- a new `readserver` binary for serving reads from in-memory table snapshots
- HTTP read endpoints:
  - `GET /healthz`
  - `GET /status`
  - `GET /read?x=<column>&y=<global_row>`
- a public Application Load Balancer for read traffic
- a readserver Auto Scaling Group for crash replacement
- a `readload` tool for generating read traffic
- AWS scripts for read load, read failover, CloudWatch graph collection, and report-ready result artifacts

The read path is designed so every read server can answer any `(x,y)` read. This makes the read tier stateless and easy to scale horizontally. If one read server crashes, the ALB removes it from rotation, the remaining read servers continue serving reads, and the Auto Scaling Group launches a replacement that reloads the latest table from S3.

Useful scripts added or updated in this branch include:

```bash
./aws-eval/12-validate-read-failover.sh
./aws-eval/13-run-read-load.sh
./aws-eval/14-run-read-load-profile.sh

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
terraform -chdir=aws-eval/terraform init -backend=false -input=false
terraform -chdir=aws-eval/terraform fmt -check
terraform -chdir=aws-eval/terraform validate
git diff --check

script/phase3-local-verify.sh
script/phase3-local-apply-scaling.sh
```

For AWS demos, run one failure demo at a time. Do not run multiple AWS eval scripts concurrently against the same `.state` directory.
