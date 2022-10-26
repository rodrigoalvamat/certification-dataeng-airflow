// Cluster VPC
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "3.18.0"

  name = "${var.project}-${var.stage}-vpc"
  cidr = "10.0.0.0/16"
  azs  = ["us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"]

  private_subnets  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets   = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]
  redshift_subnets = ["10.0.7.0/24", "10.0.8.0/24", "10.0.9.0/24"]

  enable_nat_gateway     = true
  single_nat_gateway     = true
  one_nat_gateway_per_az = false
  enable_vpn_gateway     = true

  enable_dns_hostnames = true
  enable_dns_support   = true

  enable_public_redshift = true

  tags = merge(local.common_tags, { category = "network", resource = "vpc" })
}

// Redshift subnet group
resource "aws_redshift_subnet_group" "subnet_group_redshift" {
  name       = "${var.project}-${var.stage}-subnet-group-redshift"
  subnet_ids = module.vpc.redshift_subnets

  tags = merge(local.common_tags, { category = "network", resource = "subnet-group", service = "redshift" })
}

// Redshift security group
resource "aws_security_group" "security_group_redshift" {
  description = "Allow Redshift inbound traffic"

  name   = "${var.project}-${var.stage}-security-group-redshift"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "all"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = merge(local.common_tags, { category = "network", resource = "security-group", service = "redshift" })
}
