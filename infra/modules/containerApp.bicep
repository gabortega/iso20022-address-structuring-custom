@description('Name of the Container App')
param containerAppName string

@description('Container Apps Environment resource ID')
param containerAppEnvId string

@description('Location for the resource')
param location string

@description('Environment tag')
param environment string

@description('Container image to deploy')
param containerImage string

@description('Port the container listens on')
param containerPort int

@description('ACR name')
param acrName string

@description('ACR login server')
param acrLoginServer string

@description('ACR admin password')
@secure()
param acrPassword string

@description('Maximum number of pipeline instances')
param pipelineMaxInstances string

@description('CPU per replica')
param cpuCore string

@description('Memory per replica')
param memorySize string

@description('Minimum replicas')
param minReplicas int

@description('Maximum replicas')
param maxReplicas int

resource containerApp 'Microsoft.App/containerApps@2025-07-01' = {
  name: containerAppName
  location: location
  properties: {
    managedEnvironmentId: containerAppEnvId
    configuration: {
      ingress: {
        external: true
        targetPort: containerPort
        transport: 'http2'
        allowInsecure: true
      }
      registries: [
        {
          server: acrLoginServer
          username: acrName
          passwordSecretRef: 'acr-password'
        }
      ]
      secrets: [
        {
          name: 'acr-password'
          value: acrPassword
        }
      ]
    }
    template: {
      containers: [
        {
          name: containerAppName
          image: containerImage
          env: [
            {
              name: 'ds_grpc_pipeline_max_instances'
              value: pipelineMaxInstances
            }
          ]
          resources: {
            cpu: any(cpuCore)
            memory: memorySize
          }
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
      }
    }
  }
  tags: {
    environment: environment
    app: containerAppName
  }
}