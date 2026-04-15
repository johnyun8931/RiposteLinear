#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd aws
require_cmd go
require_cmd ssh
require_cmd scp
require_cmd curl

echo "== AWS identity =="
aws_base sts get-caller-identity --output json

echo
echo "== Region =="
configured_region="$(aws configure get region || true)"
echo "configured region: ${configured_region:-<unset>}"
echo "effective region:  $AWS_REGION"

echo
echo "== Default VPC =="
aws_region ec2 describe-vpcs \
  --filters Name=is-default,Values=true \
  --query 'Vpcs[].{VpcId:VpcId,Cidr:CidrBlock,Default:IsDefault}' \
  --output table

echo
echo "== Configured subnet =="
aws_region ec2 describe-subnets \
  --subnet-ids "$SUBNET_ID" \
  --query 'Subnets[].{SubnetId:SubnetId,AZ:AvailabilityZone,Cidr:CidrBlock,MapPublicIp:MapPublicIpOnLaunch,VpcId:VpcId}' \
  --output table

echo
echo "== c5n.4xlarge offerings =="
aws_region ec2 describe-instance-type-offerings \
  --location-type availability-zone \
  --filters Name=instance-type,Values="$INSTANCE_TYPE" \
  --query 'InstanceTypeOfferings[].Location' \
  --output table

echo
echo "== Ubuntu AMI lookup =="
ami_id="$(aws_region ssm get-parameter \
  --name "$AMI_SSM_PARAM" \
  --query 'Parameter.Value' \
  --output text)"
echo "AMI: $ami_id"

echo
echo "== Current public IP =="
current_ip="$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')"
echo "$current_ip"

echo
echo "== Go package compile check =="
(cd "$REPO_ROOT" && env GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go test -vet=off ./client ./server ./utils)

echo
echo "== Linux cross-compile check =="
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go build -o "$tmpdir/server" ./server)
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go build -o "$tmpdir/client" ./client)
file "$tmpdir/server" "$tmpdir/client" || true

echo
echo "preflight passed"

