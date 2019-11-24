package com.starbucks.analytics.eventhub

import com.microsoft.azure.eventprocessorhost.{IEventProcessorFactory, PartitionContext}
import com.starbucks.analytics.keyvault.KeyVaultConnectionInfo
import com.starbucks.analytics.s3.S3ConnectionInfo

/**
  * Factory class to generate instances of Event Processor Class with constructor parameters.
  */
class EventProcessorFactory(s3ConnectionInfo: S3ConnectionInfo, keyVaultConnectionInfo: KeyVaultConnectionInfo, keyVaultResourceUri: String, desiredParallelism: Int) extends IEventProcessorFactory[EventProcessor]{

  /**
    * Method to create Event Processor instances.
    * @param context
    * @return
    */
  override def createEventProcessor(context: PartitionContext): EventProcessor = {
    new EventProcessor(s3ConnectionInfo.awsAccessKeyID, s3ConnectionInfo.awsSecretAccessKey, s3ConnectionInfo.s3BucketName, s3ConnectionInfo.s3FolderName, keyVaultConnectionInfo, keyVaultResourceUri ,desiredParallelism)
  }
}
