@description('Name of the Container Apps Environment')
param containerAppEnvName string

@description('Location for the resource')
param location string

@description('Environment tag')
param environment string

@description('Log retention in days')
param retentionDays int = 30

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2025-07-01' = {
  name: '${containerAppEnvName}-logs'
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: retentionDays
  }
  tags: {
    environment: environment
  }
}

resource containerAppEnv 'Microsoft.App/managedEnvironments@2025-07-01' = {
  name: containerAppEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
  tags: {
    environment: environment
  }
}