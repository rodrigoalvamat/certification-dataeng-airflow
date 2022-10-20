output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = aws_redshift_cluster.redshift_cluster.endpoint
}

output "redshift_role_arn" {
  description = "IAM redshift role arn"
  value       = aws_iam_role.role_redshift.arn
}
