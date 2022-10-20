// S3 buckets 
resource "aws_s3_bucket" "buckets" {
  count         = length(var.bucket_names)
  bucket        = "${var.project}-${var.stage}-bucket-${var.bucket_names[count.index]}"
  force_destroy = true

  tags = merge(local.common_tags, { category = "storage", resource = "bucket", service = "S3" })
}

// S3 buckets acl
resource "aws_s3_bucket_acl" "bucket_acl" {
  count  = length(var.bucket_names)
  bucket = "${var.project}-${var.stage}-bucket-${var.bucket_names[count.index]}"
  acl    = "public-read-write"
}

// S3 buckets public access block 
resource "aws_s3_bucket_public_access_block" "public_access_block" {
  count  = length(var.bucket_names)
  bucket = "${var.project}-${var.stage}-bucket-${var.bucket_names[count.index]}"
}
