#!/usr/bin/env bash

LAYER_ARN=$(aws-vault exec sandbox -d 18h -- aws lambda publish-layer-version --layer-name 'lambda-extension-log-shipper' --region us-east-1 --zip-file 'fileb://bin/lambda-extension-log-shipper.zip' | jq -r '.LayerVersionArn')
echo "Adding $LAYER_ARN to Lambda function..."
aws-vault exec sandbox -d 18h -- aws lambda update-function-configuration --region us-east-1 --function-name dbg-mngd-lmbd_cliappjs14_cli-251 --layers $LAYER_ARN
