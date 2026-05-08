locals {
  common_tags = {
    Project = var.project_tag
    RunId   = var.run_id
  }

  completed_upload_ledger_dynamodb_enabled = var.completed_upload_ledger_backend == "dynamodb"
  completed_upload_ledger_table_name       = var.completed_upload_ledger_table != "" ? var.completed_upload_ledger_table : var.dynamodb_control_table
  dynamodb_runtime_enabled                 = var.control_store_backend == "dynamodb" || var.session_store_backend == "dynamodb" || local.completed_upload_ledger_dynamodb_enabled
  session_table_name                       = var.dynamodb_session_table != "" ? var.dynamodb_session_table : var.dynamodb_control_table
  session_table_region                     = var.dynamodb_session_region != "" ? var.dynamodb_session_region : var.dynamodb_control_region
  dynamodb_table_arns = distinct(concat(
    var.control_store_backend == "dynamodb" ? [
      "arn:aws:dynamodb:${var.dynamodb_control_region}:${data.aws_caller_identity.current.account_id}:table/${var.dynamodb_control_table}",
    ] : [],
    var.session_store_backend == "dynamodb" ? [
      "arn:aws:dynamodb:${local.session_table_region}:${data.aws_caller_identity.current.account_id}:table/${local.session_table_name}",
    ] : [],
    local.completed_upload_ledger_dynamodb_enabled ? [
      "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${local.completed_upload_ledger_table_name}",
    ] : []
  ))
  public_entry_enabled       = var.public_entry_backend == "nlb"
  multi_coordinator          = local.public_entry_enabled && contains(["1", "true"], lower(var.public_entry_multi_coordinator))
  nlb_suffix                 = substr(var.run_id, max(0, length(var.run_id) - 16), 16)
  nlb_name                   = substr("riposte-${local.nlb_suffix}", 0, 32)
  nlb_target_group_name      = substr("riposte-tg-${local.nlb_suffix}", 0, 32)
  ingestion_sqs_enabled      = var.ingestion_queue_backend == "sqs"
  server_aws_runtime_enabled = local.ingestion_sqs_enabled || local.completed_upload_ledger_dynamodb_enabled
  ingestion_bucket_name      = var.ingestion_s3_bucket != "" ? var.ingestion_s3_bucket : lower(substr("${var.project_tag}-${var.run_id}-ingestion", 0, 63))
  ingestion_sqs_queue_arns = local.ingestion_sqs_enabled ? [
    aws_sqs_queue.ingestion_shard0[0].arn,
    aws_sqs_queue.ingestion_shard1[0].arn,
  ] : []
  ingestion_s3_object_arns = local.ingestion_sqs_enabled ? ["${aws_s3_bucket.ingestion_payloads[0].arn}/*"] : []
  ingestion_s3_bucket_arns = local.ingestion_sqs_enabled ? [aws_s3_bucket.ingestion_payloads[0].arn] : []
  server_ingestion_policy_statements = concat(
    local.ingestion_sqs_enabled ? [
      {
        Effect = "Allow"
        Action = [
          "sqs:GetQueueAttributes",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
          "sqs:DeleteMessage",
        ]
        Resource = local.ingestion_sqs_queue_arns
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
        ]
        Resource = local.ingestion_s3_object_arns
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
        ]
        Resource = local.ingestion_s3_bucket_arns
      },
    ] : [],
    local.completed_upload_ledger_dynamodb_enabled ? [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
        ]
        Resource = ["arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${local.completed_upload_ledger_table_name}"]
      },
    ] : []
  )
}

resource "aws_key_pair" "eval" {
  key_name   = var.key_name
  public_key = file(var.ssh_public_key_path)

  tags = merge(local.common_tags, {
    Name = var.key_name
  })
}

resource "aws_security_group" "eval" {
  name        = var.sg_name
  description = "Temporary Riposte AWS evaluation security group ${var.run_id}"
  vpc_id      = var.vpc_id

  tags = merge(local.common_tags, {
    Name = var.sg_name
  })
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.eval.id
  cidr_ipv4         = var.ssh_cidr
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
  description       = "SSH from eval operator"
}

resource "aws_vpc_security_group_ingress_rule" "self_tcp" {
  security_group_id            = aws_security_group.eval.id
  referenced_security_group_id = aws_security_group.eval.id
  from_port                    = 0
  to_port                      = 65535
  ip_protocol                  = "tcp"
  description                  = "All app traffic inside eval security group"
}

resource "aws_vpc_security_group_ingress_rule" "public_coordinator" {
  count = local.public_entry_enabled ? 1 : 0

  security_group_id = aws_security_group.eval.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = tonumber(var.coordinator_port)
  to_port           = tonumber(var.coordinator_port)
  ip_protocol       = "tcp"
  description       = "Riposte NLB public coordinator entry"
}

resource "aws_vpc_security_group_ingress_rule" "public_standby_coordinator" {
  count = local.multi_coordinator ? 1 : 0

  security_group_id = aws_security_group.eval.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = tonumber(var.coordinator_standby_port)
  to_port           = tonumber(var.coordinator_standby_port)
  ip_protocol       = "tcp"
  description       = "Riposte NLB public standby coordinator entry"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.eval.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "coordinator" {
  count = local.dynamodb_runtime_enabled ? 1 : 0

  name = var.coordinator_iam_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "coordinator_dynamodb" {
  count = local.dynamodb_runtime_enabled ? 1 : 0

  name = var.coordinator_iam_policy_name
  role = aws_iam_role.coordinator[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
        ]
        Resource = local.dynamodb_table_arns
      }
    ]
  })
}

resource "aws_iam_instance_profile" "coordinator" {
  count = local.dynamodb_runtime_enabled ? 1 : 0

  name = var.coordinator_iam_instance_profile_name
  role = aws_iam_role.coordinator[0].name

  tags = local.common_tags
}

resource "aws_dynamodb_table" "control" {
  provider = aws.control
  count    = var.create_dynamodb_control_table ? 1 : 0

  name         = var.dynamodb_control_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  tags = local.common_tags
}

resource "aws_dynamodb_table" "session" {
  provider = aws.session
  count    = var.create_dynamodb_session_table ? 1 : 0

  name         = local.session_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  tags = local.common_tags
}

resource "aws_sqs_queue" "ingestion_shard0" {
  count = local.ingestion_sqs_enabled ? 1 : 0

  name                       = substr("${var.project_tag}-${local.nlb_suffix}-ingestion-shard0", 0, 80)
  visibility_timeout_seconds = 300
  message_retention_seconds  = 1209600

  tags = merge(local.common_tags, {
    Role = "ingestion-shard0"
  })
}

resource "aws_sqs_queue" "ingestion_shard1" {
  count = local.ingestion_sqs_enabled ? 1 : 0

  name                       = substr("${var.project_tag}-${local.nlb_suffix}-ingestion-shard1", 0, 80)
  visibility_timeout_seconds = 300
  message_retention_seconds  = 1209600

  tags = merge(local.common_tags, {
    Role = "ingestion-shard1"
  })
}

resource "aws_s3_bucket" "ingestion_payloads" {
  count = local.ingestion_sqs_enabled ? 1 : 0

  bucket        = local.ingestion_bucket_name
  force_destroy = true

  tags = merge(local.common_tags, {
    Role = "ingestion-payloads"
  })
}

resource "aws_s3_bucket_public_access_block" "ingestion_payloads" {
  count = local.ingestion_sqs_enabled ? 1 : 0

  bucket                  = aws_s3_bucket.ingestion_payloads[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_iam_role" "server_ingestion" {
  count = local.server_aws_runtime_enabled ? 1 : 0

  name = var.server_ingestion_iam_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "server_ingestion" {
  count = local.server_aws_runtime_enabled ? 1 : 0

  name = var.server_ingestion_iam_policy_name
  role = aws_iam_role.server_ingestion[0].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.server_ingestion_policy_statements
  })
}

resource "aws_iam_instance_profile" "server_ingestion" {
  count = local.server_aws_runtime_enabled ? 1 : 0

  name = var.server_ingestion_iam_instance_profile_name
  role = aws_iam_role.server_ingestion[0].name

  tags = local.common_tags
}

resource "aws_instance" "coordinator" {
  ami                         = var.ami_id
  instance_type               = var.coordinator_instance_type
  key_name                    = aws_key_pair.eval.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  iam_instance_profile        = local.dynamodb_runtime_enabled ? aws_iam_instance_profile.coordinator[0].name : null

  tags = merge(local.common_tags, {
    Name = "${var.project_tag}-coordinator"
    Role = "coordinator"
  })

  root_block_device {
    tags = merge(local.common_tags, {
      Name = "${var.project_tag}-coordinator-root"
      Role = "coordinator"
    })
  }
}

resource "aws_instance" "shard0_leader" {
  ami                         = var.ami_id
  instance_type               = var.server_instance_type
  key_name                    = aws_key_pair.eval.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  iam_instance_profile        = local.server_aws_runtime_enabled ? aws_iam_instance_profile.server_ingestion[0].name : null

  tags = merge(local.common_tags, {
    Name = "${var.project_tag}-shard0-leader"
    Role = "shard0-leader"
  })

  root_block_device {
    tags = merge(local.common_tags, {
      Name = "${var.project_tag}-shard0-leader-root"
      Role = "shard0-leader"
    })
  }
}

resource "aws_instance" "shard0_follower" {
  ami                         = var.ami_id
  instance_type               = var.server_instance_type
  key_name                    = aws_key_pair.eval.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  iam_instance_profile        = local.server_aws_runtime_enabled ? aws_iam_instance_profile.server_ingestion[0].name : null

  tags = merge(local.common_tags, {
    Name = "${var.project_tag}-shard0-follower"
    Role = "shard0-follower"
  })

  root_block_device {
    tags = merge(local.common_tags, {
      Name = "${var.project_tag}-shard0-follower-root"
      Role = "shard0-follower"
    })
  }
}

resource "aws_instance" "shard1_leader" {
  ami                         = var.ami_id
  instance_type               = var.server_instance_type
  key_name                    = aws_key_pair.eval.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  iam_instance_profile        = local.server_aws_runtime_enabled ? aws_iam_instance_profile.server_ingestion[0].name : null

  tags = merge(local.common_tags, {
    Name = "${var.project_tag}-shard1-leader"
    Role = "shard1-leader"
  })

  root_block_device {
    tags = merge(local.common_tags, {
      Name = "${var.project_tag}-shard1-leader-root"
      Role = "shard1-leader"
    })
  }
}

resource "aws_instance" "shard1_follower" {
  ami                         = var.ami_id
  instance_type               = var.server_instance_type
  key_name                    = aws_key_pair.eval.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  iam_instance_profile        = local.server_aws_runtime_enabled ? aws_iam_instance_profile.server_ingestion[0].name : null

  tags = merge(local.common_tags, {
    Name = "${var.project_tag}-shard1-follower"
    Role = "shard1-follower"
  })

  root_block_device {
    tags = merge(local.common_tags, {
      Name = "${var.project_tag}-shard1-follower-root"
      Role = "shard1-follower"
    })
  }
}

resource "aws_instance" "client" {
  ami                         = var.ami_id
  instance_type               = var.client_instance_type
  key_name                    = aws_key_pair.eval.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true

  tags = merge(local.common_tags, {
    Name = "${var.project_tag}-client"
    Role = "client"
  })

  root_block_device {
    tags = merge(local.common_tags, {
      Name = "${var.project_tag}-client-root"
      Role = "client"
    })
  }
}

resource "aws_lb" "public" {
  count = local.public_entry_enabled ? 1 : 0

  name               = local.nlb_name
  load_balancer_type = "network"
  internal           = false
  subnets            = [var.subnet_id]

  tags = local.common_tags
}

resource "aws_lb_target_group" "coordinator" {
  count = local.public_entry_enabled ? 1 : 0

  name        = local.nlb_target_group_name
  port        = tonumber(var.coordinator_port)
  protocol    = "TCP"
  target_type = "instance"
  vpc_id      = var.vpc_id

  health_check {
    protocol = "TCP"
  }

  tags = local.common_tags
}

resource "aws_lb_target_group_attachment" "coordinator" {
  count = local.public_entry_enabled ? 1 : 0

  target_group_arn = aws_lb_target_group.coordinator[0].arn
  target_id        = aws_instance.coordinator.id
  port             = tonumber(var.coordinator_port)
}

resource "aws_lb_target_group_attachment" "coordinator_standby" {
  count = local.multi_coordinator ? 1 : 0

  target_group_arn = aws_lb_target_group.coordinator[0].arn
  target_id        = aws_instance.coordinator.id
  port             = tonumber(var.coordinator_standby_port)
}

resource "aws_lb_listener" "coordinator" {
  count = local.public_entry_enabled ? 1 : 0

  load_balancer_arn = aws_lb.public[0].arn
  port              = tonumber(var.coordinator_port)
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.coordinator[0].arn
  }
}
