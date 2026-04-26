@description('Location for all resources')
param location string

@description('Name of the Key Vault (3-24 chars, alphanumeric and hyphens)')
param keyVaultName string

@allowed(['standard', 'premium'])
param sku string

@description('Soft delete retention in days (7–90)')
param softDeleteRetentionInDays int

@description('Object ID of the Admin User')
@secure()
param userId string

@description('Object ID of the Service Principal')
@secure()
param servicePrincipalId string

@description('Tags to apply to the Container Instance')
param tags object

// List of roles assignments to add to Key Vault RBAC
var roleAssignments = [
  {
    principalId:   userId
    principalType: 'User'
    role:          '00482a5a-887f-4fb3-b363-3b7fe8e74483' // Administrator
  }
  {
    principalId:   servicePrincipalId
    principalType: 'ServicePrincipal'
    role:          '4633458b-17de-408a-b874-0445c86b69e6' // SecretUser
  }
]

resource keyVault 'Microsoft.KeyVault/vaults@2025-05-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
        family: 'A'
        name: sku
    }
    tenantId: subscription().tenantId

    enableSoftDelete: true
    enableRbacAuthorization: true
    softDeleteRetentionInDays: softDeleteRetentionInDays
    enabledForTemplateDeployment: true

    publicNetworkAccess: 'Enabled'
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
  }
}

// Add all role assignments to Key Vault RBAC
resource keyVaultRoleAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for roleAssignment in roleAssignments: {
    name: guid(keyVault.id, roleAssignment.principalId, roleAssignment.role)
    scope: keyVault
    properties: {
      roleDefinitionId: subscriptionResourceId(
        'Microsoft.Authorization/roleDefinitions',
        roleAssignment.role
      )
      principalId: roleAssignment.principalId
      principalType: roleAssignment.principalType
    }
  }
]