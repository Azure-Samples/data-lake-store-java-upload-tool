package com.starbucks.analytics

import com.microsoft.azure.eventprocessorhost.{EventProcessorHost, EventProcessorOptions}
import com.starbucks.analytics.blob.BlobConnectionInfo
import com.starbucks.analytics.eventhub._
import com.starbucks.analytics.keyvault.KeyVaultConnectionInfo
import com.starbucks.analytics.s3.S3ConnectionInfo
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionException
import scala.util.{Failure, Success}


/**
  * Main class to start file transfer from Azure blob to AWS S3.
  */
object Main {
  private val logger: Logger = Logger("Main")

  /**
    * Entry point
    * @param args Command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new s3cpConf(args)
    logger.info(conf.summary)

    //Setup Azure EventHub Connection
    val eventHubConnectionInfo = EventHubConnectionInfo(
           eventHubNamespaceName = conf.eventHubNamespaceName(),
          eventHubName = conf.eventHubName(),
         sasKeyName = conf.eventHubSASKeyName(),
          sasKey = conf.eventHubSASKey(),
          eventProcessorName = conf.eventProcessorName(),
          eventProcessorStorageContainer = conf.eventProcessorStorageContainer(),
          consumerGroup = conf.eventHubConsumerGroup()
    )

    //Setup AWS S3 Connection
    val s3ConnectionInfo = S3ConnectionInfo(
      awsAccessKeyID = conf.awsAccessKeyID(),
      awsSecretAccessKey = conf.awsSecretAccessKey(),
      s3BucketName = conf.s3BucketName(),
      s3FolderName = conf.s3FolderName()
    )

    //Setup Azure Storage connection for EventProcessor.
    val blobConnectionInfo = BlobConnectionInfo (
      accountName = conf.eventHubStorageAccountName(),
      accountKey = conf.eventHubStorageAccountKey()
    )

    val keyVaultConnectionInfo = KeyVaultConnectionInfo(
      clientId = conf.spnClientId(),
      clientKey = conf.spnClientKey()
    )

    EventHubManager.getEventProcessorHost(eventHubConnectionInfo, blobConnectionInfo) match {
      case Failure(e) => println(e)
      case Success(eventProcessorHost) => {
        val options = new EventProcessorOptions
        options.setExceptionNotification(new EventHubErrorNotificationHandler)
        try{
          eventProcessorHost.registerEventProcessorFactory(new EventProcessorFactory(s3ConnectionInfo, keyVaultConnectionInfo, conf.keyVaultResourceUri() ,conf.desiredParallelism()), options)
        }catch {
          case e: ExecutionException => {
            logger.error(e.getCause.toString)
          }
          case e: Exception => {
            logger.error(e.getCause.toString)
          }
        }
        // SIGNAL LISTENER TO GRACEFULLY TERMINATE EVENT PROCESSOR.
        println("Press any key to stop: ")
        try{
          System.in.read()
          eventProcessorHost.unregisterEventProcessor()
          logger.warn("Calling forceExecutorShutdown")
          EventProcessorHost.setAutoExecutorShutdown(true)
          logger.info("EventProcessorHost existed gracefully!")
        }catch {
          case e: Exception => {
            logger.error(e.getCause.toString)
            System.exit(1)
          }
        }
      }
    }
    System.exit(0)
  }

}
