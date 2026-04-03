#!/bin/bash
ENV_ID=$(az containerapp env show \
  --name $CONTAINER_APP_ENV_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "id" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$ENV_ID"
echo "container_app_env_id=$ENV_ID" >> $GITHUB_OUTPUT