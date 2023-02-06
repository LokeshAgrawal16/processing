#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
fi

buildname=$(echo $1)

sts_session=$(aws sts get-session-token )
aws_access_key_id=$(echo $sts_session | jq .Credentials.AccessKeyId | tr -d '"')
aws_secret_key=$(echo $sts_session | jq .Credentials.SecretAccessKey | tr -d '"')
aws_session_token=$(echo $sts_session | jq .Credentials.SessionToken | tr -d '"')

docker build  --build-arg AWS_SESSION_TOKEN=$aws_session_token --build-arg AWS_ACCESS_KEY_ID=$aws_access_key_id --build-arg AWS_SECRET_ACCESS_KEY=$aws_secret_key --build-arg AWS_DEFAULT_REGION=ap-south-1 -t $buildname .
