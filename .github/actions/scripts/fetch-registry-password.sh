#!/bin/bash

# Retrieves the first admin password for an Azure Container Registry (ACR)
# and exposes it as a GitHub Actions step output named 'registry_password'.
# The value is masked in the log to prevent accidental exposure.

# Required environment variables:
#   REGISTRY_NAME    - Name of the Azure Container Registry
#   RESOURCE_GROUP   - Azure resource group containing the registry
# Outputs (via $GITHUB_OUTPUT):
#   registry_password - The first admin credential password for the ACR
PASSWORD=$(az acr credential show \
  --name $REGISTRY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "passwords[0].value" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$PASSWORD"
echo "registry_password=$PASSWORD" >> $GITHUB_OUTPUT