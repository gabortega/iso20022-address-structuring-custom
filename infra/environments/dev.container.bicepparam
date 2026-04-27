using '../container.bicep'

param containerPort          = 8080
param environment            = 'dev'
param pipelineMaxInstances   = '1'
param cpuCore                = 2
param memoryInGB             = 3