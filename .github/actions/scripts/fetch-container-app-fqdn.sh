#!/bin/bash

# Retrieves the fully qualified domain name (FQDN) of an Azure Container App
# and exposes it as a GitHub Actions step output named 'container_app_fqdn'.
# The value is masked in the log to prevent accidental exposure.

# Required environment variables:
#   $CONTAINER_APP_NAME    - Name of the Azure Container App
#   RESOURCE_GROUP   - Azure resource group containing the Container App
# Outputs (via $GITHUB_OUTPUT):
#   container_app_fqdn - The public FQDN of the Container App ingress
FQDN=$(az containerapp show \
  --name $CONTAINER_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "properties.configuration.ingress.fqdn" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$FQDN"
echo "container_app_fqdn=$FQDN" >> $GITHUB_OUTPUT