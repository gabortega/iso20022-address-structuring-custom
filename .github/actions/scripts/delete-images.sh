#!/bin/bash

# Taken from: https://github.com/MicrosoftDocs/azure-management-docs/blob/main/articles/container-registry/container-registry-delete.md#delete-digests-by-timestamp

# WARNING! This script deletes data!
# Run only if you do not have systems
# that pull images via manifest digest.

# Change to 'true' to enable image delete
# ENABLE_DELETE=false

# Modify for your environment
# TIMESTAMP can be a date-time string such as 2023-03-15T17:55:00.
# REGISTRY_NAME=myregistry
# REPOSITORY=myrepository
TIMESTAMP=$(az acr manifest list-metadata --name $REPOSITORY --registry $REGISTRY_NAME \
            --orderby time_desc --query "[].[lastUpdateTime]" -o tsv | head -1)

# Delete all images older than specified timestamp.

if [ "$ENABLE_DELETE" = true ]
then
    az acr manifest list-metadata --name $REPOSITORY --registry $REGISTRY_NAME \
    --orderby time_asc --query "[?lastUpdateTime < '$TIMESTAMP'].digest" -o tsv \
    | xargs -I% az acr repository delete --name $REGISTRY_NAME --image $REPOSITORY@% --yes
else
    echo "No data deleted."
    echo "Set ENABLE_DELETE=true to enable deletion of these images in $REPOSITORY:"
    az acr manifest list-metadata --name $REPOSITORY --registry $REGISTRY_NAME \
   --orderby time_asc --query "[?lastUpdateTime < '$TIMESTAMP'].[digest, lastUpdateTime]" -o tsv
fi
