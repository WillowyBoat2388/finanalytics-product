resource "kubernetes_deployment" "finnhub_k8s" {
  metadata {
    name = "finnhub_k8s"
    namespace = "${var.namespace}"
    labels = {
      "k8s.service" = "finnhub_k8s"
    }
  }

  spec {
    replicas = 2

    selector {
      match_labels = {
        "k8s.service" = "finnhub_k8s"
      }
    }

    template {
      metadata {
        labels = {
          "k8s.network/pipeline-network" = "true"

          "k8s.service" = "finnhub_k8s"
        }
      }

      spec {
        container {
          name  = "finnhub_k8s_container"
          image = "docker.io/library/finnhub-batch-stock-pipeline-finnhub_k8s:v1.0.0"

          env_from {
            config_map_ref {
              name = "pipeline-config"
            }
          }

          env_from {
            secret_ref {
              name = "pipeline-secrets"
            }
          }

          image_pull_policy = "Never"
        }

        restart_policy = "Always"
      }
    }
  }
}

resource "kubernetes_service" "finnhub_k8s" {
  metadata {
    name  = "finnhub_k8s"
    namespace = "${var.namespace}"
    labels = {
      "k8s.service" = "finnhub_k8s"
    }
  }

  depends_on = [
        kubernetes_deployment.finnhub_k8s
  ]
  
  spec {
    # type = "NodePort"
    port {
      name        = "80"
      port        = 80
      target_port = 80
    }

    selector = {
      "k8s.service" = "finnhub_k8s"
    }

    # cluster_ip = "None"
  }
}