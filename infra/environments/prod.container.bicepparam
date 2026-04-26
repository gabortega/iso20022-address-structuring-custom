using '../container.bicep'

param containerPort          = 8080
param environment            = 'prod'
param pipelineMaxInstances   = '3'
param cpuCore                = 4
param memoryInGB             = 8