variable "kube_config" {
  type    = string
  default = "~/.kube/config"
}

variable "namespace" {
  type    = string
  default = "pipeline"
}

variable "AWS_SECRET_ACCESS_KEY" {
  type = string
  sensitive = true
}

variable "AWS_ACCESS_KEY_ID" {
  type = string
  sensitive = true
}

variable "AWS_S3_ALLOW_UNSAFE_RENAME" {
  type = string
}

variable "S3_URI" {
  type = string
}

variable "AWS_BUCKET" {
  type = string
}

variable "AWS_ENDPOINT" {
  type = string
}

variable "AWS_REGION" {
  type = string
}