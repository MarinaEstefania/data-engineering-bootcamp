resource "aws_s3_bucket" "bucket" {
  bucket_prefix = var.bucket_prefix

  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "s3-data-bootcamp"
  }
}

resource "aws_s3_bucket_object" "object_movie" {
  bucket   = aws_s3_bucket.bucket.id
  key      = "movie_reviews.csv"
  source   = "C:/Users/mgarc/Documents/wizeline/bootcamp/data-engineering-bootcamp/DATA/movie_review.csv"
}

resource "aws_s3_bucket_object" "object_log" {
  bucket   = aws_s3_bucket.bucket.id
  key      = "log_reviews.csv"
  source   = "C:/Users/mgarc/Documents/wizeline/bootcamp/data-engineering-bootcamp/DATA/log_reviews.csv"
}

resource "aws_s3_bucket_object" "object_purchase" {
  bucket   = aws_s3_bucket.bucket.id
  key      = "user_purchase.csv"
  source   = "C:/Users/mgarc/Documents/wizeline/bootcamp/data-engineering-bootcamp/DATA/user_purchase.csv"
}