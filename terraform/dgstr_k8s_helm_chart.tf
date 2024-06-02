# resource "helm_release" "dagster-on-k8s" {
#   name       = "dagster-on-k8s"
#   repository = "https://dagster-io.github.io/helm"
#   chart      = "dagster"
#   namespace  = "${var.namespace}"
  
#   values = [
#     file("${path.module}/values.yaml")
#   ]
  
#   # Increase the apply timeout to 10 minutes
#   timeout = "10000"
# }