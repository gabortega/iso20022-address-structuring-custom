#!/bin/bash
PASSWORD=$(az acr credential show \
  --name $REGISTRY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "passwords[0].value" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$PASSWORD"
echo "registry_password=$PASSWORD" >> $GITHUB_OUTPUT