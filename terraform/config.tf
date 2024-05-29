resource "kubernetes_namespace" "pipeline-namespace" {
  metadata {
    name = "${var.namespace}"
  }
}

resource "kubernetes_config_map" "pipeline-config" {
  metadata {
    name      = "pipeline-config"
    namespace = "${var.namespace}"
  }

  depends_on = [
        kubernetes_namespace.pipeline-namespace
  ]

  data = {
        AWS_REGION="eu-west-1"
        AWS_BUCKET="dagster-api"
        S3_URI="s3://dagster-api"
        AWS_S3_ALLOW_UNSAFE_RENAME="True"
  }
}