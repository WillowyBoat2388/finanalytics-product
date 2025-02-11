FROM python:3.11-slim

#Set base directory as environment variable wihin container
ENV PYTHONPATH="/:${PYTHONPATH}"

# ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install necessary packages
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

#Copy requirements file to docker container
COPY requirements.txt .

#Install all requirements
RUN pip install --no-cache-dir -r requirements.txt

#Get pipeline code-location
COPY ./finnhub-batch-stock-pipeline/ .