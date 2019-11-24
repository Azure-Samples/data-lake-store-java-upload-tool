package com.starbucks.analytics

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

/**
 * Represents the scallop configuration class for the application
 *
 * @param arguments Command line arguments
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    s"""
       |Tool to copy securely from Azure Data Lake Store to Blob Store
       |Example:
       |java -jar $getApplicationName
       |  --spnClientId xx
       |  --spnClientKey xx
       |  --spnOauth2TokenEndpoint xx
       |  --adlsFQDN xx
       |  --blobStoreAccountName xx
       |  --blobStoreAccountKey xx
       |  --sourceFile xx
       |  --blobStoreContainerName xx
       |  --blobStoreRootFolder xx
       |  --tokenExpirationInMinutes 00
       |  --keyVaultResourceUri xx
       |  --eventHubNamespaceName xx
       |  --eventHubName xx
       |  --eventHubSASKeyName xx
       |  --eventHubSASKey xx
       |  --desiredParallelism 1
       |  --desiredBufferSize 1
       |Options:
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
  val spnOauth2TokenEndpoint = opt[String](
    name = "spnOauth2TokenEndpoint",
    noshort = true,
    descr = "Authentication Token Endpoint of the Azure active directory application",
    required = true
  )
  val adlsFQDN = opt[String](
    name = "adlsFQDN",
    noshort = true,
    descr = "Fully Qualified Domain Name of the Azure data lake account",
    required = true
  )
  val blobStoreAccountName = opt[String](
    name = "blobStoreAccountName",
    noshort = true,
    descr = "Name of the blob storage account (destination)",
    required = true
  )
  val blobStoreAccountKey = opt[String](
    name = "blobStoreAccountKey",
    noshort = true,
    descr = "Key to access the blob storage account (destination)",
    required = true
  )
  val sourceFolder = opt[String](
    name = "sourceFolder",
    noshort = true,
    descr = "Folder in the Azure Data Lake Store that contains the file(s) to copy",
    required = false
  )
  val sourceFile = opt[String](
    name = "sourceFile",
    noshort = true,
    descr = "File in the Azure Data Lake Store to copy",
    required = false
  )
  val blobStoreContainerName = opt[String](
    name = "blobStoreContainerName",
    noshort = true,
    descr = "Name of the blob store container (destination)",
    required = true
  )
  val blobStoreRootFolder = opt[String](
    name = "blobStoreRootFolder",
    noshort = true,
    descr = "Root folder in the blob store container (destination)",
    required = false
  )
  val tokenExpiration = opt[Int](
    name = "tokenExpirationInMinutes",
    noshort = true,
    descr = "Please specify the token expiration time in minutes",
    required = true
  )
  val keyVaultResourceUri = opt[String](
    name = "keyVaultResourceUri",
    noshort = true,
    descr = "Key Vault resource uri",
    required = true
  )
  val eventHubNamespaceName = opt[String](
    name = "eventHubNamespaceName",
    noshort = true,
    descr = "Namespace of the event hub",
    required = false
  )
  val eventHubName = opt[String](
    name = "eventHubName",
    noshort = true,
    descr = "Name of the event hub",
    required = false
  )
  val eventHubSASKeyName = opt[String](
    name = "eventHubSASKeyName",
    noshort = true,
    descr = "Shared Access Signature key name for accessing the event hub",
    required = false
  )
  val eventHubSASKey = opt[String](
    name = "eventHubSASKey",
    noshort = true,
    descr = "Shared Access Signature Key for accessing the event hub",
    required = false
  )
  val desiredParallelism = opt[Int](
    name = "desiredParallelism",
    noshort = true,
    descr =
      """Desired level of parallelism.
        |This will impact your available network bandwidth and source system resources""".stripMargin,
    required = false,
    default = Some(Runtime.getRuntime.availableProcessors())
  )
  val desiredBufferSize = opt[Int](
    name = "desiredBufferSize",
    noshort = true,
    descr = "Desired buffer size in megabytes.This will impact your available network bandwidth.",
    required = false,
    default = Some(8)
  )

  footer("\nFor all other questions, consult the documentation or contact the author!")

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
  }

  dependsOnAny(adlsFQDN, List(sourceFolder, sourceFile))
  verify()

  private def getApplicationName: String = new java.io.File(classOf[App]
    .getProtectionDomain
    .getCodeSource
    .getLocation
    .getPath)
    .getName
}