#!/bin/bash

# Retrieves the IP of an Azure Container Instance
# and exposes it as a GitHub Actions step output named 'container_instance_ip'.
# The value is masked in the log to prevent accidental exposure.

# Required environment variables:
#   RESOURCE_GROUP              - Azure resource group containing the Container Instance
#   CONTAINER_INSTANCE_NAME     - Name of the Azure Container Instance Group
# Outputs (via $GITHUB_OUTPUT):
#   container_instance_ip       - The public IP of the Container Instance
IP=$(az container show \
  --resource-group $RESOURCE_GROUP \
  --name $CONTAINER_INSTANCE_NAME \
  --query "ipAddress.ip" -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$IP"
echo "container_instance_ip=$IP" >> $GITHUB_OUTPUT