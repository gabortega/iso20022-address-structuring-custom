@description('Location for all resources')
param location string

@description('Name of the Azure Container Registry')
param acrName string

@description('Name of the Managed Identity with pull assignment')
param managedIdentityName string

@description('Name of the Container Instance')
param containerInstanceName string

@description('Container image to deploy (full path with tag)')
param containerImage string

@description('gRPC port the container listens on')
param containerPort int

@description('Maximum number of pipeline instances')
param pipelineMaxInstances string

@description('CPU cores')
param cpuCore int

@description('Memory in GB')
param memoryInGB int

@description('Tags to apply to the Container Instance')
param tags object

// Existing ACR reference
resource acr 'Microsoft.ContainerRegistry/registries@2026-01-01-preview' existing = {
  name: acrName
}

// Existing managed identity reference
resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  name: managedIdentityName
}

resource containerGroup 'Microsoft.ContainerInstance/containerGroups@2025-09-01' = {
  name: containerInstanceName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    containers: [
      {
        name: containerInstanceName
        properties: {
          command: []
          environmentVariables: [
            {
              name: 'ds_grpc_pipeline_max_instances'
              value: pipelineMaxInstances
            }
          ]
          image: '${acr.properties.loginServer}/${containerImage}'
          ports: [
            {
              port: containerPort
              protocol: 'TCP'
            }
          ]
          resources: {
            limits: {
              cpu: any(cpuCore)
              memoryInGB: memoryInGB
            }
            requests: {
              cpu: any(cpuCore)
              memoryInGB: memoryInGB
            }
          }
        }
      }
    ]
    initContainers: []
    ipAddress: {
      ports: [
        {
          port: containerPort
          protocol: 'TCP'
        }
      ]
      type: 'Public'
    }
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        identity: identity.id
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Always'
    volumes: []
  }
  zones: []
}