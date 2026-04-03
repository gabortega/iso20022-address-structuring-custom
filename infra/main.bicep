// ============================================================
// Phase 1 — Infrastructure (ACR + Log Analytics + Container App Environment)
// ============================================================

@description('Location for all resources')
param location string = resourceGroup().location

@description('Name of the Azure Container Registry')
param acrName string = ''

@description('Name of the Container Apps Environment')
param containerAppEnvName string = ''

@description('Environment tag')
@allowed(['dev', 'staging', 'prod'])
param environment string

// ============================================================
// Modules
// ============================================================

module acr 'modules/acr.bicep' = {
  name: 'acr'
  params: {
    acrName: acrName
    location: location
    environment: environment
  }
}

module containerAppEnv 'modules/containerAppEnv.bicep' = {
  name: 'containerAppEnv'
  params: {
    containerAppEnvName: containerAppEnvName
    location: location
    environment: environment
  }
}