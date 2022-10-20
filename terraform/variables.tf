// Project global variables
variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "stage" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Resources prefix"
  type        = string
  default     = "udacity-airflow"
}

variable "bucket_names" {
  description = "S3 bucket names"
  type        = list(string)
  default     = [
    "landing",
    "bronze",
    "silver",
    "gold",
    "scripts"
  ]
}

// Project and stage common tags
locals {
  common_tags = {
    project = var.project
    stage   = var.stage
  }
}

// Redshift sensitive information
variable "redshift_database" {
  description = "Redshift database name"
  type        = string
  sensitive   = true
}

variable "redshift_user" {
  description = "Redshift admin user name"
  type        = string
  sensitive   = true
}

variable "redshift_password" {
  description = "Redshift admin password"
  type        = string
  sensitive   = true
}
