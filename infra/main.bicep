// ============================================================
// Phase 1 — Infrastructure (Key Vault + ACR)
// ============================================================

// ============================================================
// Parameters
// ============================================================

@description('Location for all resources')
param location string = resourceGroup().location

@description('Environment tag')
@allowed(['dev', 'staging', 'prod'])
param environment string

@description('Name of the Key Vault (3-24 chars, alphanumeric and hyphens)')
param keyVaultName string = ''

@allowed(['standard', 'premium'])
param keyVaultSku string = 'standard'

@description('Soft delete retention in days (7–90)')
@minValue(7)
@maxValue(90)
param softDeleteRetentionInDays int = 90

@description('Object ID of the Admin User')
@secure()
param userId string = ''

@description('Object ID of the Service Principal')
@secure()
param servicePrincipalId string = ''

@description('Name of the Azure Container Registry')
param acrName string = ''

@allowed(['Basic', 'Classic', 'Standard', 'Premium'])
param acrSku string = 'Basic'

param utcShort string = utcNow('d')

// ============================================================
// Variables
// ============================================================

// Tags to apply to resources
var tags = {
  Environment: environment
  ManagedBy: 'Bicep'
  LastDeployed: utcShort
}

// ============================================================
// Resources
// ============================================================

module keyVault 'modules/keyVault.bicep' = {
  name: 'deployKeyVault'
  params: {
    location: location
    keyVaultName: keyVaultName
    sku: keyVaultSku
    softDeleteRetentionInDays: softDeleteRetentionInDays
    userId: userId
    servicePrincipalId: servicePrincipalId
    tags: tags
  }
}

module acr 'modules/acr.bicep' = {
  name: 'deployACR'
  params: {
    location: location
    keyVaultName: keyVaultName
    acrName: acrName
    sku: acrSku
    tags: tags
  }
}