resource "kubernetes_pod" "nfs-server" {
  metadata {
    name = "nfs-server"
    namespace = var.namespace
    labels = {
      role = "nfs-server"
    }
  }

  spec {
    container {
      name  = "nfs-server"
      image = "itsthenetwork/nfs-server-alpine:latest"

      env {
        name = "SHARED_DIRECTORY"
        value = "data"
      }
      
      port {
        name           = "nfs"
        container_port = 2049
      }
      security_context {
        privileged = true
      }
      volume_mount {
        name       = "nfs-data"
        mount_path = "/data"
      }
    }

    volume {
      name = "nfs-data"
      empty_dir {}
    }
  }
}

resource "terraform_data" "nfs-ip" {
  depends_on = [kubernetes_pod.nfs-server]
  
# Provisioner to retrieve the IP address using kubectl
  provisioner "local-exec" {
    command = "kubectl get pod -n dagster nfs-server -o jsonpath='{.status.podIP}' > ${path.module}/nfs_ip.txt"
    interpreter = ["bash", "-c"]
    on_failure = continue
  }
}

data "local_file" "provisioner_output" {
  depends_on = [ terraform_data.nfs-ip, kubernetes_pod.nfs-server ]
  filename = "${path.module}/nfs_ip.txt"
}

# Set the local variable with the retrieved IP address
# Define a local variable to store the IP address
locals {
  nfs_pod_ip = data.local_file.provisioner_output.content
}

output "provisioner_output" {
  value = data.local_file.provisioner_output.content
}

resource "kubernetes_persistent_volume" "nfs-pv" {
  metadata {
    name = "nfs-pv"
  }

  depends_on = [ kubernetes_pod.nfs-server, terraform_data.nfs-ip, data.local_file.provisioner_output ]

  spec {
    capacity = {
      storage = "1Gi"
    }
    access_modes = ["ReadWriteMany"]

    persistent_volume_source {
        nfs {
      server = local.nfs_pod_ip
      path   = "/data"
        }
    }
  }
}

resource "kubernetes_persistent_volume_claim" "nfs-pvc" {
  metadata {
    name = "nfs-pvc"
    namespace = var.namespace
  }

  depends_on = [ kubernetes_persistent_volume.nfs-pv ]

  spec {
    access_modes = ["ReadWriteMany"]

    resources {
      requests = {
        storage = "1Gi"
      }
    }
  }
}

# resource "kubernetes_service" "nfs-server" {
#   metadata {
#     name = "nfs-server"
#   }

#   spec {
#     selector = {
#       role = "nfs-server"
#     }

#     port {
#       name       = "nfs"
#       port       = 2049
#       target_port = 2049
#     }
#   }
# }

# data "kubernetes_service" "nfs-server" {
#   metadata {
#     name = kubernetes_service.nfs-server.metadata[0].name
#   }
# }
