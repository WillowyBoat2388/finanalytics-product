# resource "kubernetes_secret" "pipeline-secrets" {
#   metadata {
#     name      = "pipeline-secrets"
#     namespace = "${var.namespace}"
#   }

#   depends_on = [
#         kubernetes_namespace.pipeline-namespace
#   ]

#   data = {
#     dagster-aws-access-key-id = "${var.AWS_ACCESS_KEY_ID}"
#     dagster-aws-secret-access-key = "${var.AWS_SECRET_ACCESS_KEY}"
#   }

#   type = "opaque"
# }