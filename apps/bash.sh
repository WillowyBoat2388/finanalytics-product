#!/bin/bash

sudo apt -y upgrade && sudo apt-get install -y screen 

# Assign script directory path to variable
SCRIPT_DIR=$(dirname "$BASH_SOURCE")

# download minio server and make it executable
wget -P $SCRIPT_DIR/ https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x $SCRIPT_DIR/minio

# download minio client and make it executable
wget -P $SCRIPT_DIR/ https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x $SCRIPT_DIR/mc

# download and install minikube 
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64

# download and install terraform
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && apt install terraform

# download docker-compose and make executable
# sudo curl -L "https://github.com/docker/compose/releases/download/1.25.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
# && sudo chmod +x /usr/local/bin/docker-compose

# sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# assign minio server start command to variable
minio_startcommand="$SCRIPT_DIR/minio server $SCRIPT_DIR/../data --console-address :9001"

echo "Starting command: $minio_startcommand"

# Create a new screen session named "minioscreen" and run the minio server command within it
screen -dmS minioscreen bash -c "$minio_startcommand"
sleep 10

# assign the minio host port to alias:  minio
$SCRIPT_DIR/mc alias set minio http://127.0.0.1:9000 minioadmin minioadmin
# return info regarding created minio client alias and save to txt file
$SCRIPT_DIR/mc admin info minio > minio.txt
#create bucket dagster-api within created minio client
$SCRIPT_DIR/mc mb minio/dagster-api

# assign minikube start command to variable
minikube_startcommand="minikube start"

# Create a new screen session named "minikubescreen" and run a command
screen -dmS minikubescreen bash -c "$minikube_startcommand"

wait_for_screen() {
    local screen_name=$1
    while screen -list | grep -q "$screen_name"; do
        echo "Waiting for the screen session '$screen_name' to complete..."
        sleep 5
    done
}

# Wait for the "minioscreen" to finish
wait_for_screen "minikubescreen"

# sudo apt install apache2-utils

# htpasswd -Bc finnhub-batch-stock-pipeline/apps/my-pdr/a2auth/registry.password dagster

# set docker-environment to be within minikube
eval $(minikube docker-env)

# create docker image within minikube container
docker build -t fnhb-btch-stck-ppln .

# minikube cache add <my-docker-image>

# minikube image load <image name>


cd terraform

# terraform plan

# terraform import kubernetes_secret.pipeline-secrets 

# terraform apply