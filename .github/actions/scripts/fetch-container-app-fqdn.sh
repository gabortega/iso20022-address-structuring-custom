#!/bin/bash
FQDN=$(az containerapp show \
  --name $CONTAINER_APP \
  --resource-group $RESOURCE_GROUP \
  --query "properties.configuration.ingress.fqdn" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$FQDN"
echo "container_app_fqdn=$FQDN" >> $GITHUB_OUTPUT