#!/bin/bash

wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server ../data --console-address :9001
> echo $! > minio.txt

wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc

./mc alias set minio http://127.0.0.1:9000 minioadmin minioadmin
./mc admin info minio
./mc mb minio/dagster-api

