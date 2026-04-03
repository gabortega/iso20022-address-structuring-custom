#!/bin/bash

# Retrieves the login server URL of an Azure Container Registry (ACR)
# and exposes it as a GitHub Actions step output named 'registry_url'.
# The value is masked in the log to prevent accidental exposure.

# Required environment variables:
#   REGISTRY_NAME    - Name of the Azure Container Registry
#   RESOURCE_GROUP   - Azure resource group containing the registry
# Outputs (via $GITHUB_OUTPUT):
#   registry_url - The ACR login server URL (e.g. myregistry.azurecr.io)
LOGIN_URL=$(az acr show \
  --name $REGISTRY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "loginServer" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$LOGIN_URL"
echo "registry_url=$LOGIN_URL" >> $GITHUB_OUTPUT