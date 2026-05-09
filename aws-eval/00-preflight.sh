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
require_cmd file

echo "== AWS identity =="
aws_base sts get-caller-identity --output json

echo
echo "== Region =="
configured_region="$(aws_base configure get region || true)"
echo "configured region: ${configured_region:-<unset>}"
echo "effective region:  $AWS_REGION"

echo
echo "== Selected network =="
IFS=$'\t' read -r resolved_vpc resolved_subnet resolved_az <<<"$(resolve_network_selection)"
echo "vpc:    $resolved_vpc"
echo "subnet: $resolved_subnet"
echo "az:     $resolved_az"
aws_region ec2 describe-subnets \
  --subnet-ids "$resolved_subnet" \
  --query 'Subnets[].{SubnetId:SubnetId,AZ:AvailabilityZone,Cidr:CidrBlock,MapPublicIp:MapPublicIpOnLaunch,VpcId:VpcId}' \
  --output table

echo
echo "== Instance offerings in selected AZ =="
for instance_type in "$COORDINATOR_INSTANCE_TYPE" "$SERVER_INSTANCE_TYPE" "$CLIENT_INSTANCE_TYPE"; do
  echo "$instance_type"
  aws_region ec2 describe-instance-type-offerings \
    --location-type availability-zone \
    --filters Name=instance-type,Values="$instance_type" Name=location,Values="$resolved_az" \
    --query 'InstanceTypeOfferings[].Location' \
    --output table
done

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
echo "== Go package test/compile check =="
(cd "$REPO_ROOT" && env GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go test -vet=off ./db ./client ./server ./coordinator)

echo
echo "== Linux cross-compile check =="
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go build -o "$tmpdir/server" ./server)
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go build -o "$tmpdir/client" ./client)
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-preflight}" go build -o "$tmpdir/coordinator" ./coordinator)
file "$tmpdir/server" "$tmpdir/client" "$tmpdir/coordinator" || true

echo
echo "preflight passed"
