// ============================================================
// Phase 2 — Container App only
// ============================================================

@description('Location for all resources')
param location string = resourceGroup().location

@description('Name of the Container App')
param containerAppName string = ''

@description('Container image to deploy (full path with tag)')
param containerImage string = ''

@description('gRPC port the container listens on')
param containerPort int = 8080

@description('Environment tag')
@allowed(['dev', 'staging', 'prod'])
param environment string

@description('ACR name')
param acrName string = ''

@description('ACR login server')
param acrLoginServer string = ''

@description('ACR admin password')
@secure()
param acrPassword string = ''

@description('Container Apps Environment resource ID')
param containerAppEnvId string = ''

@description('Maximum number of pipeline instances')
param pipelineMaxInstances string = '1'

@description('CPU per replica')
param cpuCore string = '2'

@description('Memory per replica')
param memorySize string = '4Gi'

@description('Minimum replicas')
param minReplicas int = 1

@description('Maximum replicas')
param maxReplicas int = 1

// ============================================================
// Module
// ============================================================

module containerApp 'modules/containerApp.bicep' = {
  name: 'containerApp'
  params: {
    containerAppName: containerAppName
    containerAppEnvId: containerAppEnvId
    location: location
    environment: environment
    containerImage: containerImage
    containerPort: containerPort
    acrName: acrName
    acrLoginServer: acrLoginServer
    acrPassword: acrPassword
    pipelineMaxInstances: pipelineMaxInstances
    cpuCore: cpuCore
    memorySize: memorySize
    minReplicas: minReplicas
    maxReplicas: maxReplicas
  }
}