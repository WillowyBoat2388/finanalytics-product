resource "helm_release" "dagster-on-k8s" {
  name       = "dagster-on-k8s"
  repository = "https://dagster-io.github.io/helm"
  chart      = "dagster"
  namespace  = "dagster"
  create_namespace = true
  
  values = [
    file("${path.module}/values.yaml")
  ]

}