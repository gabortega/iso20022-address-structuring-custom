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
param environment string = 'dev'

@description('Name of the Azure Container Registry')
param acrName string = ''

@description('Name of the Managed Identity with pull assignment')
param managedIdentityName string = ''

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
// Modules
// ============================================================

module containerGroup 'modules/containerDeployment.bicep' = {
  name: 'deployContainer'
  params: {
    location: location
    acrName: acrName
    managedIdentityName: managedIdentityName
    containerInstanceName: containerInstanceName
    containerImage: containerImage
    containerPort: containerPort
    pipelineMaxInstances: pipelineMaxInstances
    cpuCore: cpuCore
    memoryInGB: memoryInGB
    tags: tags
  }
}