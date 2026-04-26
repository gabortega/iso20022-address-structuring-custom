// ============================================================
// Phase 2 — Container Instance only
// ============================================================

// ============================================================
// Parameters
// ============================================================

@description('Location for all resources')
param location string = resourceGroup().location

@description('Environment tag')
@allowed(['dev', 'staging', 'prod'])
param environment string

@description('Name of the Key Vault')
param keyVaultName string = ''

@description('Name of the Container Instance')
param containerInstanceName string = ''

@description('Container image to deploy (full path with tag)')
param containerImage string = ''

@description('gRPC port the container listens on')
param containerPort int = 8080

@description('Maximum number of pipeline instances')
param pipelineMaxInstances string = '1'

@description('CPU cores')
param cpuCore int = 2

@description('Memory in GB')
param memoryInGB int = 4

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

// Reference existing Key Vault
resource keyVault 'Microsoft.KeyVault/vaults@2025-05-01' existing = {
  name: keyVaultName
}

module containerGroup 'modules/containerDeployment.bicep' = {
  name: 'deployContainer'
  params: {
    location: location
    containerInstanceName: containerInstanceName
    containerImage: containerImage
    containerPort: containerPort
    server: keyVault.getSecret('acr-login-server')
    username: keyVault.getSecret('acr-username')
    password: keyVault.getSecret('acr-password')
    pipelineMaxInstances: pipelineMaxInstances
    cpuCore: cpuCore
    memoryInGB: memoryInGB
    tags: tags
  }
}