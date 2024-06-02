resource "kubernetes_deployment" "finnhub-k8s" {
  metadata {
    name = "finnhub-k8s"
    namespace = "${var.namespace}"
    labels = {
      "k8s.service" = "finnhub-k8s"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        "k8s.service" = "finnhub-k8s"
      }
    }

    template {
      metadata {
        labels = {
          "k8s.network/pipeline-network" = "true"

          "k8s.service" = "finnhub-k8s"
        }
      }

      spec {
        container {
          name  = "finnhub-k8s-container"
          image = "docker.io/dagster/fnhb-btch-stck-ppln:v1.0.0"
  
          # env_from {
          #   config_map_ref {
          #     name = "pipeline-config"
          #   }
          # }

          # env_from {
          #   secret_ref {
          #     name = "pipeline-secrets"
          #   }
          # }

          # resources {
          #   limits = {
          #     cpu    = "0.5"
          #     memory = "512Mi"
          #   }
          #   requests = {
          #     cpu    = "250m"
          #     memory = "50Mi"
          #   }
          # }

          image_pull_policy = "Never"
        }
      }
    }
  }
}

resource "kubernetes_service" "finnhub-k8s" {
  metadata {
    name  = "finnhub-k8s"
    namespace = "${var.namespace}"
    labels = {
      "k8s.service" = "finnhub-k8s"
    }
  }

  depends_on = [
        kubernetes_deployment.finnhub-k8s
  ]
  
  spec {
    type = "NodePort"
    port {
      name        = "80"
      port        = 80
      target_port = 80
    }

    selector = {
      "k8s.service" = "finnhub-k8s"
    }

    # cluster_ip = "None"
  }
}



