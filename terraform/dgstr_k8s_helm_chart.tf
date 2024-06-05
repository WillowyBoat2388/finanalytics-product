resource "helm_release" "dagster-on-k8s" {
  name       = "dagster-on-k8s"
  repository = "https://dagster-io.github.io/helm"
  chart      = "dagster"
  namespace  = "${var.namespace}"
  
  depends_on = [ kubernetes_persistent_volume_claim.nfs-pvc ]
  
  values = [
    file("${path.module}/values.yaml")
  ]
  
}