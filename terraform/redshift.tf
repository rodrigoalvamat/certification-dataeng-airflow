// Redshift Data Warehouse cluster
resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier = "${var.project}-${var.stage}-redshift-cluster"
  node_type          = "dc2.large"
  cluster_type       = "single-node"
  number_of_nodes    = 1

  enhanced_vpc_routing = true
  skip_final_snapshot  = true

  database_name   = var.redshift_database
  master_username = var.redshift_user
  master_password = var.redshift_password

  vpc_security_group_ids    = [aws_security_group.security_group_redshift.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.subnet_group_redshift.name
  iam_roles                 = [aws_iam_role.role_redshift.arn]

  tags = merge(local.common_tags, { category = "database", resource = "redshift" })
}
