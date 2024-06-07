FROM python:3.11-slim

#Set base directory as environment variable wihin container
ENV PYTHONPATH="/:${PYTHONPATH}"

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install necessary packages
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

#Copy requirements file to docker container
COPY requirements.txt .

#Install all requirements
RUN pip install --no-cache-dir -r requirements.txt

# Clear Ivy cache
RUN rm -rf /root/.ivy2/cache && rm -rf /root/.ivy2/jars

#Get pipeline code-location
COPY ./finnhub-batch-stock-pipeline/ .