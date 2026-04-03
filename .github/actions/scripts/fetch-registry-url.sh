#!/bin/bash
LOGIN_URL=$(az acr show \
  --name $REGISTRY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "loginServer" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$LOGIN_URL"
echo "registry_url=$LOGIN_URL" >> $GITHUB_OUTPUT