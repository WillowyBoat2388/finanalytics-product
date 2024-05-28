FROM python:3.11-slim

#Set base directory as environment variable wihin container
ENV PYTHONPATH="/:${PYTHONPATH}"

#Copy requirements file to docker container
COPY requirements.txt .

#Upgrade pip installation
RUN python -m pip install --upgrade pip

#Install all requirements
RUN pip install --no-cache-dir -r requirements.txt

#Get pipeline code-location
COPY ./finnhub-batch-stock-pipeline/ /