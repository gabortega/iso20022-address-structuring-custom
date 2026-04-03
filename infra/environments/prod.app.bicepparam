using '../app.bicep'

param containerPort          = 8080
param environment            = 'prod'
param pipelineMaxInstances   = '3'
param cpuCore                = '4'
param memorySize             = '8Gi'
param minReplicas            = 1
param maxReplicas            = 1