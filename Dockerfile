            FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=nonintercative

ARG AWS_DEFAULT_REGION
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_SESSION_TOKEN

RUN echo $AWS_DEFAULT_REGION
RUN echo $AWS_ACCESS_KEY_ID
RUN echo $AWS_SECRET_ACCESS_KEY
RUN echo $AWS_SESSION_TOKEN

RUN apt-get update && apt-get install --no-install-recommends -y git python3.9 python3-pip python3.9-dev curl awscli cron python3-dev gcc libcurl4-openssl-dev libssl-dev jq && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN cp /usr/share/zoneinfo/Asia/Kolkata /etc/localtime && echo "Asia/Kolkata" > /etc/timezone && mkdir -p /var/run/celery && mkdir -p /var/log/celery/  && mkdir -p /var/www/scrapy && ln -s /usr/bin/python3 /usr/bin/python

RUN pip install pipenv && pip install -U setuptools && pip install --upgrade awscli

WORKDIR /var/www/vinstore/

COPY . .

RUN aws sts get-caller-identity

RUN git config --global credential.helper '!aws codecommit credential-helper $@'

RUN git config --global credential.UseHttpPath true

RUN pipenv install --system --deploy --verbose