provider "aws" {
  region = var.region
}

terraform {
  backend s3 {
    region = "us-west-2" # This can be sourced from AWS_REGION
  }
}