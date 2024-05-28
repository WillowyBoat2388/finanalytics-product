terraform {
  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }

    helm = {
      source = "hashicorp/helm"
      version = ">= 2.0.0"
    }

    # kubectl = {
    #   source  = "gavinbunney/kubectl"
    #   version = ">= 1.7.0"
    # }
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