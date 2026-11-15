#!/bin/bash
# Deploy the training Cloud Function to GCP.
# Run this after editing functions/nba-train/main.py.
# Your function URL will be printed at the end — paste it into your .env file.

set -e

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
FUNCTION_NAME="nba-train-${USER}"

echo "Deploying ${FUNCTION_NAME} to project=${PROJECT} region=${REGION}..."

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=python311 \
  --region=${REGION} \
  --source=functions/nba-train \
  --entry-point=task \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512MB \
  --timeout=120s

echo ""
echo "Done. Your function URL:"
gcloud functions describe ${FUNCTION_NAME} --region=${REGION} --format="value(serviceConfig.uri)"
echo ""
echo "Paste that URL into your .env file as TRAIN_FUNCTION_URL"
