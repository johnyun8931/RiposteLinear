# Riposte AWS Evaluation

This directory contains a small AWS CLI runbook for reproducing a budget-limited
Riposte throughput experiment on EC2. It is intentionally separate from the
existing `script/` directory, which contains older project scripts.

The workflow uses the `eval/pre-john` baseline and the original direct TLS/RPC
transport. It does not use John Yun's HTTPS/ALB changes, Terraform, ALB, or WAN
traffic shaping.

## What This Measures

The main benchmark is the built-in `65,536` row table configuration in
`db/types.go`, with `160` byte rows. The scripts collect:

- requests/sec over time from server logs;
- average, standard deviation, min, max, and sample count;
- total processed requests during each hammer phase;
- run metadata such as instance IDs, private IPs, git commit, table size, and
  commands;
- one idle `openssl speed -evp aes-128-ctr` result from each server as an AES
  upper-bound reference.

The OpenSSL measurement is independent of the Riposte benchmark. It should be
used as bottleneck context, not as a direct measurement of Go/Riposte code.

## Cost And Runtime

The intended deployment is four On-Demand `c5n.4xlarge` instances in
`us-east-1`: two servers and two clients. With the current budget target, aim to
finish in 3-4 hours and treat 6 hours as a hard maximum including setup and
debugging.

AWS Budgets can lag. The real cost control is prompt teardown.

## Generated Local Files

Generated files are ignored by git:

- `.state/` stores the current run's AWS IDs and IPs.
- `keys/` stores the temporary SSH PEM.
- `bin/` stores Linux binaries.
- `results/` stores copied logs, parsed CSVs, summaries, and metadata.

## Prerequisites

- AWS CLI v2 authenticated as the dedicated experiment user.
- IAM permissions for EC2 and SSM read-only AMI lookup.
- `go`, `ssh`, `scp`, `curl`, and `python3`.
- Default AWS region should be `us-east-1`, or set `AWS_REGION=us-east-1`.

Verify identity:

```sh
aws --no-cli-pager sts get-caller-identity
```

Expected account/user shape:

```json
{
  "Account": "450792539324",
  "Arn": "arn:aws:iam::450792539324:user/riposte-eval-user"
}
```

## 1. Preflight

Run:

```sh
./aws-eval/00-preflight.sh
```

This checks AWS identity, default VPC/subnet, `c5n.4xlarge` availability, Ubuntu
AMI lookup through SSM, Go tests for deployment-relevant packages, and Linux
cross-compilation.

## 2. Launch EC2 Resources

Run:

```sh
./aws-eval/01-launch.sh
```

This creates:

- one temporary AWS key pair;
- one security group allowing SSH from your current public IP;
- four tagged EC2 instances.

State is written to:

```text
aws-eval/.state/env.sh
```

All resources are tagged with `Project=riposte-eval`.

## 3. Deploy Binaries

Run:

```sh
./aws-eval/02-deploy.sh
```

This builds Linux `amd64` `server` and `client` binaries locally and copies them
to the EC2 machines.

## 4. Smoke Test

Run:

```sh
./aws-eval/03-smoke.sh
```

This starts both servers and runs one non-hammer client request. Non-hammer mode
means one upload request, then the client exits. This proves SSH, binaries,
private IPs, TLS/RPC, and server wiring are working before a sustained run.

## 5. Benchmark

Run:

```sh
./aws-eval/04-run-benchmark.sh
```

Default phases:

- `sanity-10m`: 10 minutes of hammer load, about 60 server samples.
- `measured-30m`: 30 minutes of hammer load, about 180 server samples.

Hammer mode starts 16 client goroutines per client process and sends requests as
fast as possible until stopped.

For a short test of the script itself, override durations:

```sh
SANITY_SECONDS=60 MEASURED_SECONDS=120 ./aws-eval/04-run-benchmark.sh
```

## 6. Collect Logs And Parse Metrics

Run:

```sh
./aws-eval/05-collect-logs.sh
```

This copies `/tmp/riposte-eval` from each EC2 node into a timestamped local
results directory and writes:

- `metadata.json`
- `state-env.sh`
- `throughput.csv`
- `throughput-summary.txt`
- raw server/client logs
- raw OpenSSL output

## 7. Teardown

Run teardown even if a previous step fails:

```sh
./aws-eval/06-teardown.sh
```

This terminates instances, deletes the AWS key pair, and deletes the security
group after instances detach.

Then verify there are no running experiment instances:

```sh
aws --no-cli-pager ec2 describe-instances \
  --region us-east-1 \
  --filters Name=tag:Project,Values=riposte-eval Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].{InstanceId:InstanceId,State:State.Name,Name:Tags[?Key==`Name`]|[0].Value}' \
  --output table
```

## Useful Overrides

```sh
AWS_REGION=us-east-1
AWS_PROFILE=default
SUBNET_ID=subnet-0d6798aac2c2708ec
INSTANCE_TYPE=c5n.4xlarge
SANITY_SECONDS=600
MEASURED_SECONDS=1800
THREADS=16
```

Use `SUBNET_ID=... ./aws-eval/01-launch.sh` to retry another default subnet if
`c5n.4xlarge` capacity is unavailable in the preferred subnet.
