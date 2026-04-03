using '../app.bicep'

param containerPort          = 8080
param environment            = 'dev'
param pipelineMaxInstances   = '1'
param cpuCore                = '2'
param memorySize             = '4Gi'
param minReplicas            = 1
param maxReplicas            = 1