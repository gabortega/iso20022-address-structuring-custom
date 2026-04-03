#!/bin/bash

# Retrieves the full Azure resource ID of an Azure Container Apps environment
# and exposes it as a GitHub Actions step output named 'container_app_env_id'.
# The value is masked in the log to prevent accidental exposure.

# Required environment variables:
#   CONTAINER_APP_ENV_NAME - Name of the Azure Container Apps environment
#   RESOURCE_GROUP         - Azure resource group containing the environment
# Outputs (via $GITHUB_OUTPUT):
#   container_app_env_id - The full ARM resource ID of the Container Apps environment
ENV_ID=$(az containerapp env show \
  --name $CONTAINER_APP_ENV_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "id" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$ENV_ID"
echo "container_app_env_id=$ENV_ID" >> $GITHUB_OUTPUT