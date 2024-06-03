FROM python:3.11-slim

#Set base directory as environment variable wihin container
ENV PYTHONPATH="/:${PYTHONPATH}"

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install necessary packages
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

#Copy requirements file to docker container
COPY requirements.txt .

#Upgrade pip installation
RUN python -m pip install --upgrade pip

#Install all requirements
RUN pip install --no-cache-dir -r requirements.txt

#Get pipeline code-location
COPY ./finnhub-batch-stock-pipeline/ .