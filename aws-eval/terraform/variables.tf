variable "aws_region" {
  type = string
}

variable "project_tag" {
  type = string
}

variable "run_id" {
  type = string
}

variable "ami_id" {
  type = string
}

variable "ami_ssm_param" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "availability_zone" {
  type = string
}

variable "ssh_cidr" {
  type = string
}

variable "ssh_user" {
  type = string
}

variable "key_name" {
  type = string
}

variable "key_file" {
  type = string
}

variable "ssh_public_key_path" {
  type = string
}

variable "sg_name" {
  type = string
}

variable "coordinator_instance_type" {
  type = string
}

variable "server_instance_type" {
  type = string
}

variable "client_instance_type" {
  type = string
}

variable "server_threads" {
  type = string
}

variable "client_threads" {
  type = string
}

variable "client_concurrency" {
  type = string
}

variable "client_retry_overload" {
  type = string
}

variable "client_overload_backoff_initial_ms" {
  type = string
}

variable "client_overload_backoff_max_ms" {
  type = string
}

variable "warmup_epoch_seconds" {
  type = string
}

variable "measured_epoch_seconds" {
  type = string
}

variable "start_epoch_retry_timeout" {
  type = string
}

variable "start_epoch_retry_interval" {
  type = string
}

variable "post_epoch_flush_seconds" {
  type = string
}

variable "client_exit_grace_seconds" {
  type = string
}

variable "coordinator_port" {
  type = string
}

variable "coordinator_standby_port" {
  type = string
}

variable "shard0_leader_port" {
  type = string
}

variable "shard0_follower_port" {
  type = string
}

variable "shard1_leader_port" {
  type = string
}

variable "shard1_follower_port" {
  type = string
}

variable "remote_root" {
  type = string
}

variable "remote_bin_dir" {
  type = string
}

variable "remote_phases_dir" {
  type = string
}

variable "remote_smoke_dir" {
  type = string
}

variable "control_store_backend" {
  type = string
}

variable "dynamodb_control_table" {
  type = string
}

variable "dynamodb_control_region" {
  type = string
}

variable "session_store_backend" {
  type = string
}

variable "dynamodb_session_table" {
  type = string
}

variable "dynamodb_session_region" {
  type = string
}

variable "create_dynamodb_control_table" {
  type = bool
}

variable "create_dynamodb_session_table" {
  type = bool
}

variable "coordinator_holder_id" {
  type = string
}

variable "coordinator_lease_ttl_seconds" {
  type = string
}

variable "coordinator_lease_renew_seconds" {
  type = string
}

variable "coordinator_iam_role_name" {
  type = string
}

variable "coordinator_iam_instance_profile_name" {
  type = string
}

variable "coordinator_iam_policy_name" {
  type = string
}

variable "public_entry_backend" {
  type = string
}

variable "public_entry_multi_coordinator" {
  type = string
}

variable "ingestion_queue_backend" {
  type    = string
  default = "memory"
}

variable "ingestion_s3_bucket" {
  type    = string
  default = ""
}

variable "ingestion_receive_batch_size" {
  type    = number
  default = 1
}

variable "ingestion_sqs_wait_seconds" {
  type    = number
  default = 10
}

variable "ingestion_sqs_visibility_timeout_seconds" {
  type    = number
  default = 300
}

variable "ingestion_worker_error_backoff_ms" {
  type    = number
  default = 250
}

variable "server_ingestion_iam_role_name" {
  type    = string
  default = "riposte-server-ingestion"
}

variable "server_ingestion_iam_instance_profile_name" {
  type    = string
  default = "riposte-server-ingestion"
}

variable "server_ingestion_iam_policy_name" {
  type    = string
  default = "RiposteCompletedUploadIngestion"
}
