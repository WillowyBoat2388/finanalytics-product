terraform {
  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
    }

    helm = {
      source = "hashicorp/helm"
    }
    
  }
}


provider "kubernetes" {
  config_path    = pathexpand(var.kube_config)
  config_context = "minikube"
}

provider "helm" {
  kubernetes {
    config_path = pathexpand(var.kube_config)
  }
}

#this one is used to deploy DagsterChart properly
# provider "kubectl" {}