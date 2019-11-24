package com.starbucks.analytics

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

/**
  * Represents the scallop configuration class for the application
  *
  * @param arguments Command line arguments
  */
class s3cpConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    s"""
       |Tool to copy securely from Azure Blob Store to Amazon s3 bucket
       |Example:
       |Java -jar $getApplicationName
       |  --spnClientId xx
       |  --spnClientKey xx
       |  --eventHubNamespaceName xx
       |  --eventHubName xx
       |  --eventHubSASKeyName xx
       |  --eventHubSASKey xx
       |  --eventProcessorName xx
       |  --eventHubConsumerGroup xx
       |  --eventHubStorageAccountName xx
       |  --eventHubStorageAccountKey xx
       |  --eventProcessorStorageContainer xx
       |  --keyVaultResourceUri xx
       |  --awsAccessKeyID xx
       |  --awsSecretAccessKey xx
       |  --s3BucketName xx
       |  --s3FolderName xx
       |  --desiredParallelism xx
       |  Options:
       |
     """.stripMargin
  )

  val spnClientId = opt[String](
    name = "spnClientId",
    noshort = true,
    descr = "Client Id of the Azure active directory application",
    required = true
  )
  val spnClientKey = opt[String](
    name = "spnClientKey",
    noshort = true,
    descr = "Client key for the Azure active directory application",
    required = true
  )
  val eventHubNamespaceName = opt[String](
    name = "eventHubNamespaceName",
    noshort = true,
    descr = "Azure EventHub Name Space",
    required = true
  )
  val eventHubName = opt[String](
    name = "eventHubName",
    noshort = true,
    descr = "Azure EventHub to listen",
    required = true
  )
  val eventHubSASKeyName = opt[String](
    name = "eventHubSASKeyName",
    noshort = false,
    descr = "Azure EventHub Shared Access Key name",
    required = true
  )
  val eventHubSASKey = opt[String](
    name = "eventHubSASKey",
    noshort = false,
    descr = "Azure EventHub Shared Access Key",
    required = true
  )
  val eventProcessorName = opt[String](
    name = "eventProcessorName",
    noshort = false,
    descr = "Azure EventProcessor Name to register with Host",
    required = false,
    default = Some("defaultEventHubProcessor")
  )
  val eventHubConsumerGroup = opt[String](
    name = "eventHubConsumerGroup",
    noshort = false,
    descr = "Azure EventHub Consumer group to receive events",
    required = false,
    default = Some("$Default")
  )
  val eventHubStorageAccountName = opt[String](
    name = "eventHubStorageAccountName",
    noshort = false,
    descr = "Azure EventProcessor Checkpoint and Lease storage account name",
    required = true
  )
  val eventHubStorageAccountKey = opt[String](
    name = "eventHubStorageAccountKey",
    noshort = false,
    descr = "Azure EventProcessor Checkpoint and lease storage account key",
    required = true
  )
  val eventProcessorStorageContainer = opt[String](
    name = "eventProcessorStorageContainer",
    noshort = false,
    descr = "Azure EventProcessor Checkpoint and lease storage container",
    required = false,
    default = Some("defaultEventHubProcessorContainer")
  )
  val keyVaultResourceUri = opt[String](
    name = "keyVaultResourceUri",
    noshort = true,
    descr = "Key Vault resource uri",
    required = true
  )
  val awsAccessKeyID = opt[String](
    name = "awsAccessKeyID",
    noshort = false,
    descr = "AWS AccessKeyId of user",
    required = true
  )
  val awsSecretAccessKey = opt[String](
    name = "awsSecretAccessKey",
    noshort = false,
    descr = "AWS SecretAccessKey of user",
    required = true
  )
  val s3BucketName = opt[String](
    name = "s3BucketName",
    noshort = false,
    descr = "AWS S3 Bucket name",
    required = true
  )
  val s3FolderName = opt[String](
    name = "s3FolderName",
    noshort = false,
    descr = "AWS S3 Folder name",
    required = true
  )
  val desiredParallelism = opt[Int](
    name = "desiredParallelism",
    noshort = false,
    descr = "Desired number of files for parallel uploading",
    required = false,
    default = Some(8)
  )

  footer("\nFor all other questions, consult the documentation or contact the author!")

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
  }

  dependsOnAll(eventHubNamespaceName, List(spnClientId, spnClientKey, eventHubName, eventHubSASKey, eventHubSASKeyName, eventHubStorageAccountKey,
  eventHubStorageAccountName, keyVaultResourceUri, awsAccessKeyID, awsSecretAccessKey, s3BucketName, s3FolderName))
  verify()

  private def getApplicationName: String = new java.io.File(classOf[App]
    .getProtectionDomain
    .getCodeSource
    .getLocation
    .getPath)
    .getName
}
