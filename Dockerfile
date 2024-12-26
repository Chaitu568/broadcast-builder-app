FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    cron \
    awscli \
    && rm -rf /var/lib/apt/lists/* && apt-get clean 

EXPOSE 8080 8081 8001

COPY . /app 

RUN cd source && pip install -r requirements.txt 

RUN ln -sf /dev/stdout /app/source/logs_blb_app.log

ENTRYPOINT /bin/bash initiate.sh



