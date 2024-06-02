resource "kubernetes_namespace" "pipeline-namespace" {
  metadata {
    name = "${var.namespace}"
  }
}

# resource "kubernetes_config_map" "pipeline-config" {
#   metadata {
#     name      = "pipeline-config"
#     namespace = "${var.namespace}"
#   }

#   depends_on = [
#         kubernetes_namespace.pipeline-namespace
#   ]

#   data = {
#         AWS_REGION=var.AWS_REGION
#         AWS_ENDPOINT=var.AWS_ENDPOINT
#         AWS_BUCKET=var.AWS_BUCKET
#         S3_URI=var.S3_URI
#         AWS_S3_ALLOW_UNSAFE_RENAME=var.AWS_S3_ALLOW_UNSAFE_RENAME
#   }
# }