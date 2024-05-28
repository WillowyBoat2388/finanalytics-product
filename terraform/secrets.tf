resource "kubernetes_secret" "pipeline-secrets" {
  metadata {
    name      = "pipeline-secrets"
    namespace = "${var.namespace}"
  }

  depends_on = [
        "kubernetes_namespace.pipeline-namespace"
  ]

  data = {
    # username = "admin"
    # password = "P4ssw0rd"
    dagster-aws-access-key-id = ""
    dagster-aws-secret-access-key = ""
  }

  type = "opaque"
}