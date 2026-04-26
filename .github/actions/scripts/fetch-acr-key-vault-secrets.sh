#!/bin/bash

# Retrieves the full ACR credentials from the Key Vault and exposes it as
# a GitHub Actions step output named 'acr_login_url', 'acr_username', 'acr_password'.
# The value is masked in the log to prevent accidental exposure.

# Required environment variables:
#   KEY_VAULT       - Name of the Azure Key Vault with the ACR secrets
# Outputs (via $GITHUB_OUTPUT):
#   acr_login_url   - The URL of the ACR login server
#   acr_username    - The username to login to the ACR
#   acr_password    - The password to login to the ACR
ACR_LOGIN_URL=$(az keyvault secret show \
  --vault-name $KEY_VAULT \
  --name "acr-login-server" \
  --query value -o tsv)
ACR_USERNAME=$(az keyvault secret show \
  --vault-name $KEY_VAULT \
  --name "acr-username" \
  --query value -o tsv)
ACR_PASSWORD=$(az keyvault secret show \
  --vault-name $KEY_VAULT \
  --name "acr-password" \
  --query value -o tsv)

# Mask the value so it never appears in logs
echo "::add-mask::$ACR_LOGIN_URL"
echo "::add-mask::$ACR_USERNAME"
echo "::add-mask::$ACR_PASSWORD"
{
  echo "registry_url=$ACR_LOGIN_URL"
  echo "registry_username=$ACR_USERNAME"
  echo "registry_password=$ACR_PASSWORD"
} >> "$GITHUB_OUTPUT"