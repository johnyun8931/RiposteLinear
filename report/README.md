# Riposte AWS Architecture Report README

## High-Level Deployment Description

This project deploys a dynamically sharded, epoch-based Riposte-style anonymous write relay on AWS. Clients submit writes through the write ingress path, where a Network Load Balancer forwards custom TCP/TLS RPC traffic to coordinator processes. Coordinators route each upload to the correct write shard using the current shard topology, persist shared session state in DynamoDB, and forward the existing `Upload1` / `Upload2` / `Upload3` protocol to shard leaders.

Each write shard is deployed as a leader/follower pair. After a shard accepts a completed `Upload3`, the shard writes the completed-upload payload to S3 and sends compact pointer messages through shard-specific SQS queues. Workers consume those messages, load the payload from S3, process the completed upload, and acknowledge the SQS message only after successful processing. Hot standby ingestion can send the same pointer to active and standby queues so a standby replica stays caught up for promotion.

Reads use a separate path. Published table artifacts are stored in S3, and stateless read servers load those artifacts into memory. Clients reach read servers through an Application Load Balancer using HTTP JSON requests. This split keeps the existing write protocol on NLB/custom RPC while using ALB/HTTP for stateless reads, semantic health checks, and cleaner CloudWatch metrics.

## Cloud Services Used

- **EC2:** Runs coordinator processes, shard leader/follower processes, read servers, workers, client/load generators, and autoscaler processes.
- **Network Load Balancer:** Provides Layer 4 ingress for the custom TCP/TLS coordinator write protocol.
- **Application Load Balancer:** Provides Layer 7 HTTP ingress for stateless read requests and `/healthz` checks.
- **DynamoDB:** Stores coordinator lease/fencing state, epoch metadata, shard topology, immutable epoch topology snapshots, coordinator sessions, completed-upload ledger records, scaling recommendations, and epoch-cycle state.
- **S3:** Stores completed-upload payloads after accepted `Upload3` and stores published table/result artifacts used by read servers.
- **SQS:** Carries shard-specific completed-upload pointer messages for durable post-`Upload3` processing and redelivery.
- **IAM:** Grants EC2 runtime roles scoped access to DynamoDB, S3, and SQS resources.
- **CloudWatch:** Stores logs and metrics used for evidence screenshots, failure validation, scaling validation, and throughput summaries.
- **Terraform:** Creates and tears down the repeatable AWS evaluation infrastructure.

## Scaling and Failure Demonstrations

Scaling was demonstrated at epoch boundaries. After an epoch completed, the coordinator computed accepted-request density and persisted a scaling recommendation. Scaling is intentionally not applied during an active epoch. Riposte writes choose rows uniformly at random across the active global row space, and each shard's epoch batch acts as part of the anonymity set. Changing the shard map mid-epoch would split one random write process across two topologies, potentially leaving a newly activated shard with too few writes and making per-shard anonymity guarantees harder to reason about. For validation, the scaling threshold was intentionally configured to make a short AWS run trigger a `grow` recommendation. The apply path then changed the authoritative shard configuration from one active shard to two active shards, increasing global table height from 256 to 512. The report evidence shows the recommendation, density, shard-count change, global-table-height change, and `scaling_applied` cycle state.

Failure behavior was demonstrated with bounded AWS scenarios:

- **Coordinator failover:** A coordinator holding the DynamoDB lease was stopped during an active epoch. A standby coordinator later acquired the lease using DynamoDB fencing while shared sessions allowed upload routing to continue through available coordinators.
- **Shard hot standby promotion:** The active shard leader was stopped. The coordinator detected the failure, promoted the hot standby shard replica, and a subsequent write reached the promoted standby.
- **SQS redelivery/idempotence:** The ingestion worker was forced to fail one SQS acknowledgement after processing. SQS redelivered the pointer, the worker detected the completed-upload ledger entry, skipped duplicate processing, and acknowledged the redelivered message.

The main remaining limitation is that partial shard-side `Upload1` / `Upload2` state is not fully durably replayed after shard death. The durable replay boundary begins after a shard accepts `Upload3` and writes the completed-upload payload to S3/SQS.

## Division of Work

- **Kevin Jin:** Implemented and evaluated the write path. Responsibilities included coordinator routing, sharded write topology, DynamoDB control/session state, coordinator lease/fencing, SQS/S3 completed-upload ingestion, completed-upload ledger behavior, scaling recommendation/apply flow, failure demonstrations, AWS write-path validation, and the main report integration.

- **John Yun:** Implemented and evaluated the read path. Responsibilities included moving published server data to S3, building the read-server path that loads S3-published table artifacts, exposing the HTTP JSON read API, configuring ALB-based read ingress and health checks, and validating that clients can read published data from the S3-backed read tier.
