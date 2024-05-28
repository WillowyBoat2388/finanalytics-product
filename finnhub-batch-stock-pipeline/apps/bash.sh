#!/bin/bash

SCRIPT_DIR=$(dirname "$BASH_SOURCE")

wget -P $SCRIPT_DIR/ https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x $SCRIPT_DIR/minio
# > echo $! > minio.txt
wget -P $SCRIPT_DIR/ https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x $SCRIPT_DIR/mc
# ./mc alias set minio http://127.0.0.1:9000 minioadmin minioadmin

curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64

wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform

sudo curl -L "https://github.com/docker/compose/releases/download/1.25.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
&& sudo chmod +x /usr/local/bin/docker-compose

sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

$SCRIPT_DIR/minio server ../data --console-address :9001
$SCRIPT_DIR/mc alias set minio http://localhost:9000 minioadmin minioadmin
$SCRIPT_DIR/mc admin info minio > minio.txt
$SCRIPT_DIR/mc mb minio/dagster-api

minikube start
# sudo apt install apache2-utils

# htpasswd -Bc finnhub-batch-stock-pipeline/apps/my-pdr/a2auth/registry.password dagster


eval $(minikube docker-env)

docker build -t <image-tag> <my-docker-image>

minikube cache add <my-docker-image>

minikube image load <image name>

cd terraform

terraform import kubernetes_secret.pipeline-secrets 

terraform apply