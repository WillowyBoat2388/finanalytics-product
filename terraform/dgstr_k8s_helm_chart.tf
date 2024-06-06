resource "helm_release" "dagster-on-k8s" {
  name       = "dagster-on-k8s"
  repository = "https://dagster-io.github.io/helm"
  chart      = "dagster"
  namespace  = "${var.namespace}"
  
  depends_on = [ kubernetes_persistent_volume_claim.nfs-pvc ]
  
  values = [
    file("${path.module}/values.yaml")
  ]

  set {
    name  = "run_launcher.config.job_image"
    value = "spark:python3-java17"
  }

  # set {
  #   name  = "k8sRunLauncher.image.tag"
  #   value = ""
  # }

  set {
    name  = "run_launcher.config.image_pull_policy"
    value = "IfNotPresent"
  }
}