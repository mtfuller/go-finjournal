# Reference AWS benchmark environment (RFC Section 13):
# - finjournald: c7i.4xlarge (16 vCPU)
# - Postgres 17: r7i.4xlarge (16 vCPU, 128 GB) on io2 with 64k provisioned IOPS
# - Load generator: c7i.2xlarge

terraform {
  required_version = ">= 1.5.0"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "instance_type_app" {
  type    = string
  default = "c7i.4xlarge"
}

variable "instance_type_db" {
  type    = string
  default = "r7i.4xlarge"
}

variable "instance_type_loadgen" {
  type    = string
  default = "c7i.2xlarge"
}
