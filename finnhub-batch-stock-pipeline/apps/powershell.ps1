& minikube -p minikube docker-env --shell powershell | Invoke-Expression
minikube docker-env | Invoke-Expression # PowerShell

docker build -t <image-tag> <my-docker-image>

minikube cache add <my-docker-image>

minikube image load <image name>
