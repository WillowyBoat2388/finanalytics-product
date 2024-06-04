resource "kubernetes_secret" "pipeline-secrets" {
  metadata {
    name      = "pipeline-secrets"
    namespace = "${var.namespace}"
  }

  depends_on = [
        kubernetes_namespace.pipeline-namespace
  ]

  data = {
    AWS_ACCESS_KEY_ID = "${var.AWS_ACCESS_KEY_ID}"
    AWS_SECRET_ACCESS_KEY = "${var.AWS_SECRET_ACCESS_KEY}"
    FINNHUBAPIKEY = "${var.FINNHUBAPIKEY}"
  }

  type = "opaque"
}