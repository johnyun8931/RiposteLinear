output "state_env" {
  value = {
    RUN_ID      = var.run_id
    PROJECT_TAG = var.project_tag
    AWS_REGION  = var.aws_region

    AMI_ID        = var.ami_id
    AMI_SSM_PARAM = var.ami_ssm_param

    SELECTED_VPC_ID    = var.vpc_id
    SELECTED_SUBNET_ID = var.subnet_id
    SELECTED_AZ        = var.availability_zone

    KEY_NAME = aws_key_pair.eval.key_name
    KEY_FILE = var.key_file

    SG_ID    = aws_security_group.eval.id
    SG_NAME  = var.sg_name
    SSH_CIDR = var.ssh_cidr

    COORDINATOR_INSTANCE_TYPE = var.coordinator_instance_type
    SERVER_INSTANCE_TYPE      = var.server_instance_type
    CLIENT_INSTANCE_TYPE      = var.client_instance_type

    COORDINATOR_ID     = aws_instance.coordinator.id
    SHARD0_LEADER_ID   = aws_instance.shard0_leader.id
    SHARD0_FOLLOWER_ID = aws_instance.shard0_follower.id
    SHARD1_LEADER_ID   = aws_instance.shard1_leader.id
    SHARD1_FOLLOWER_ID = aws_instance.shard1_follower.id
    CLIENT_ID          = aws_instance.client.id

    COORDINATOR_PRIVATE_IP     = aws_instance.coordinator.private_ip
    SHARD0_LEADER_PRIVATE_IP   = aws_instance.shard0_leader.private_ip
    SHARD0_FOLLOWER_PRIVATE_IP = aws_instance.shard0_follower.private_ip
    SHARD1_LEADER_PRIVATE_IP   = aws_instance.shard1_leader.private_ip
    SHARD1_FOLLOWER_PRIVATE_IP = aws_instance.shard1_follower.private_ip
    CLIENT_PRIVATE_IP          = aws_instance.client.private_ip

    COORDINATOR_PUBLIC_IP     = aws_instance.coordinator.public_ip
    SHARD0_LEADER_PUBLIC_IP   = aws_instance.shard0_leader.public_ip
    SHARD0_FOLLOWER_PUBLIC_IP = aws_instance.shard0_follower.public_ip
    SHARD1_LEADER_PUBLIC_IP   = aws_instance.shard1_leader.public_ip
    SHARD1_FOLLOWER_PUBLIC_IP = aws_instance.shard1_follower.public_ip
    CLIENT_PUBLIC_IP          = aws_instance.client.public_ip

    SSH_USER = var.ssh_user

    SERVER_THREADS                     = var.server_threads
    CLIENT_THREADS                     = var.client_threads
    CLIENT_CONCURRENCY                 = var.client_concurrency
    CLIENT_RETRY_OVERLOAD              = var.client_retry_overload
    CLIENT_OVERLOAD_BACKOFF_INITIAL_MS = var.client_overload_backoff_initial_ms
    CLIENT_OVERLOAD_BACKOFF_MAX_MS     = var.client_overload_backoff_max_ms
    WARMUP_EPOCH_SECONDS               = var.warmup_epoch_seconds
    MEASURED_EPOCH_SECONDS             = var.measured_epoch_seconds
    START_EPOCH_RETRY_TIMEOUT          = var.start_epoch_retry_timeout
    START_EPOCH_RETRY_INTERVAL         = var.start_epoch_retry_interval
    POST_EPOCH_FLUSH_SECONDS           = var.post_epoch_flush_seconds
    CLIENT_EXIT_GRACE_SECONDS          = var.client_exit_grace_seconds

    COORDINATOR_PORT         = var.coordinator_port
    COORDINATOR_STANDBY_PORT = var.coordinator_standby_port
    SHARD0_LEADER_PORT       = var.shard0_leader_port
    SHARD0_FOLLOWER_PORT     = var.shard0_follower_port
    SHARD1_LEADER_PORT       = var.shard1_leader_port
    SHARD1_FOLLOWER_PORT     = var.shard1_follower_port

    REMOTE_ROOT       = var.remote_root
    REMOTE_BIN_DIR    = var.remote_bin_dir
    REMOTE_PHASES_DIR = var.remote_phases_dir
    REMOTE_SMOKE_DIR  = var.remote_smoke_dir

    CONTROL_STORE_BACKEND   = var.control_store_backend
    DYNAMODB_CONTROL_TABLE  = var.dynamodb_control_table
    DYNAMODB_CONTROL_REGION = var.dynamodb_control_region
    SESSION_STORE_BACKEND   = var.session_store_backend
    DYNAMODB_SESSION_TABLE  = local.session_table_name
    DYNAMODB_SESSION_REGION = local.session_table_region

    COORDINATOR_HOLDER_ID                 = var.coordinator_holder_id
    COORDINATOR_LEASE_TTL_SECONDS         = var.coordinator_lease_ttl_seconds
    COORDINATOR_LEASE_RENEW_SECONDS       = var.coordinator_lease_renew_seconds
    COORDINATOR_IAM_ROLE_NAME             = var.coordinator_iam_role_name
    COORDINATOR_IAM_INSTANCE_PROFILE_NAME = var.coordinator_iam_instance_profile_name
    COORDINATOR_IAM_POLICY_NAME           = var.coordinator_iam_policy_name

    PUBLIC_ENTRY_BACKEND           = var.public_entry_backend
    PUBLIC_ENTRY_MULTI_COORDINATOR = var.public_entry_multi_coordinator
    NLB_NAME                       = local.public_entry_enabled ? aws_lb.public[0].name : ""
    NLB_ARN                        = local.public_entry_enabled ? aws_lb.public[0].arn : ""
    NLB_DNS_NAME                   = local.public_entry_enabled ? aws_lb.public[0].dns_name : ""
    NLB_TARGET_GROUP_NAME          = local.public_entry_enabled ? aws_lb_target_group.coordinator[0].name : ""
    NLB_TARGET_GROUP_ARN           = local.public_entry_enabled ? aws_lb_target_group.coordinator[0].arn : ""
    NLB_LISTENER_ARN               = local.public_entry_enabled ? aws_lb_listener.coordinator[0].arn : ""

    INGESTION_QUEUE_BACKEND                    = var.ingestion_queue_backend
    INGESTION_S3_BUCKET                        = local.ingestion_sqs_enabled ? aws_s3_bucket.ingestion_payloads[0].bucket : ""
    INGESTION_RECEIVE_BATCH_SIZE               = var.ingestion_receive_batch_size
    INGESTION_SQS_WAIT_SECONDS                 = var.ingestion_sqs_wait_seconds
    INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS   = var.ingestion_sqs_visibility_timeout_seconds
    INGESTION_WORKER_ERROR_BACKOFF_MS          = var.ingestion_worker_error_backoff_ms
    INGESTION_SQS_SHARD0_QUEUE_URL             = local.ingestion_sqs_enabled ? aws_sqs_queue.ingestion_shard0[0].url : ""
    INGESTION_SQS_SHARD0_QUEUE_ARN             = local.ingestion_sqs_enabled ? aws_sqs_queue.ingestion_shard0[0].arn : ""
    INGESTION_SQS_SHARD1_QUEUE_URL             = local.ingestion_sqs_enabled ? aws_sqs_queue.ingestion_shard1[0].url : ""
    INGESTION_SQS_SHARD1_QUEUE_ARN             = local.ingestion_sqs_enabled ? aws_sqs_queue.ingestion_shard1[0].arn : ""
    COMPLETED_UPLOAD_LEDGER_BACKEND            = var.completed_upload_ledger_backend
    COMPLETED_UPLOAD_LEDGER_TABLE              = local.completed_upload_ledger_table_name
    COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS    = var.completed_upload_processing_ttl_seconds
    SERVER_INGESTION_IAM_ROLE_NAME             = var.server_ingestion_iam_role_name
    SERVER_INGESTION_IAM_INSTANCE_PROFILE_NAME = var.server_ingestion_iam_instance_profile_name
    SERVER_INGESTION_IAM_POLICY_NAME           = var.server_ingestion_iam_policy_name
  }
}
