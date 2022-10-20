// AWS provider
provider "aws" {
  region = var.region
}

// IAM role for Redshift 
resource "aws_iam_role" "role_redshift" {
  description = "Provides write permissions to CloudWatch Logs and S3 Full Access"

  name               = "${var.project}-${var.stage}-iam-role-redshift"
  path               = "/"
  assume_role_policy = file("./iam/role-redshift.json")

  tags = merge(local.common_tags, { category = "iam", resource = "role", service = "redshift" })
}

// IAM policy for Redshift
resource "aws_iam_policy" "policy_redshift" {
  description = "Provides write permissions to CloudWatch Logs and S3 Full Access"

  name   = "${var.project}-${var.stage}-iam-policy-redshift"
  path   = "/"
  policy = file("./iam/policy-redshift.json")
}

// IAM role policy attachment for Redshift
resource "aws_iam_role_policy_attachment" "role_redshift_attach" {
  role       = aws_iam_role.role_redshift.name
  policy_arn = aws_iam_policy.policy_redshift.arn
}

// IAM user for Redshift
resource "aws_iam_user" "user_redshift" {
  name = "${var.project}-${var.stage}-iam-user-redshift"
  path = "/"
}

// IAM access key for Redshift user
resource "aws_iam_access_key" "user_key_redshift" {
  user = aws_iam_user.user_redshift.name
}

// IAM user policy for Redshift
resource "aws_iam_user_policy" "user_policy_redshift" {
  name = "${var.project}-${var.stage}-iam-user-policy-redshift"
  user = aws_iam_user.user_redshift.name

  policy = file("./iam/user-redshift.json")
}
