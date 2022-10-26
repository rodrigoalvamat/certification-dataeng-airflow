output "redshift_endpoint" {
  description = "Redshift cluster endpoint"
  value       = aws_redshift_cluster.redshift_cluster.endpoint
}

// Saves Redshift cluster config to redshift.cfg
resource "local_file" "emr_cluster_dns" {
  depends_on = [aws_redshift_cluster.redshift_cluster]
  filename   = "../config/redshift.cfg"

  content = <<EOT
[REDSHIFT]
DNS=${aws_redshift_cluster.redshift_cluster.endpoint}
EOT
}
