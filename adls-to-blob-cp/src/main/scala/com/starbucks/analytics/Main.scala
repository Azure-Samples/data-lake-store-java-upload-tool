package com.starbucks.analytics

import com.microsoft.azure.datalake.store.ADLFileInputStream
import com.microsoft.azure.storage.{ AccessCondition, OperationContext }
import com.microsoft.azure.storage.blob.{ BlobEncryptionPolicy, BlobRequestOptions, CloudBlockBlob }
import com.starbucks.analytics.adls.{ ADLSConnectionInfo, ADLSManager }
import com.starbucks.analytics.blob.{ BlobConnectionInfo, BlobManager }
import com.starbucks.analytics.eventhub.{ Event, EventHubConnectionInfo, EventHubManager }
import com.starbucks.analytics.keyvault.{ KeyVaultConnectionInfo, KeyVaultManager }
import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParSeq
import scala.util.{ Failure, Success, Try }

/**
 * Entry point for the application
 */
object Main {
  val logger = Logger("Main")
  var successMap = new mutable.LinkedHashMap[String, (Boolean, Option[(String, String)])]()
  var renamesuccessMap = new mutable.LinkedHashMap[String, Boolean]()

  /**
   * Entry point
   * @param args Command line arguments
   */
  def main(args: Array[String]) {
    val conf = new Conf(args)
    logger.info(conf.summary)

    // Setup ADLS Connection Information
    val adlsConnectionInfo = ADLSConnectionInfo(
      clientId = conf.spnClientId(),
      clientKey = conf.spnClientKey(),
      authenticationTokenEndpoint = conf.spnOauth2TokenEndpoint(),
      accountFQDN = conf.adlsFQDN()
    )
    // Setup BLOB Store Connection Information
    val blobConnectionInfo = BlobConnectionInfo(
      accountName = conf.blobStoreAccountName(),
      accountKey = conf.blobStoreAccountKey()
    )
    // Setup Key Vault Connection Information
    val keyVaultConnectionInfo = KeyVaultConnectionInfo(
      clientId = conf.spnClientId(),
      clientKey = conf.spnClientKey()
    )
    // Setup Event Hub Connection Information
    val eventHubConnectionInfo = EventHubConnectionInfo(
      eventHubNamespaceName = conf.eventHubNamespaceName(),
      eventHubName = conf.eventHubName(),
      sasKeyName = conf.eventHubSASKeyName(),
      sasKey = conf.eventHubSASKey()
    )

    // Get a list of files from the source
    val listOfFiles = getListOfFiles(
      conf,
      adlsConnectionInfo
    )
    if (listOfFiles.isEmpty) {
      logger.error("List of source files is empty. Quitting the application.")
      System.exit(-1)
    }

    // Setup parallelism
    val parallelListOfFiles: ParSeq[String] = listOfFiles.par
    parallelListOfFiles.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(conf.desiredParallelism())
    )

    // Start the parallel operation and collect the results
    parallelListOfFiles
      .foreach(file => {
        logger.info(s"Operating on source file $file" +
          s" in Azure Data Lake Store ${adlsConnectionInfo.accountFQDN}")
        val result: (Boolean, Option[(String, String)]) = upload(
          adlsConnectionInfo,
          blobConnectionInfo,
          keyVaultConnectionInfo,
          conf,
          file
        )
        successMap += (file -> result)
      })

    // Validate the results
    // and publish events to event hub
    if (successMap.exists((x) => !x._2._1)) {
      logger.error(s"Failed uploading the following files:" +
        s" ${successMap.filter((x) => !x._2._1).mkString("\n")}")
      System.exit(-1)
    } else {
      logger.info(s"Succeeded uploading the files: " +
        s" ${successMap.keys.mkString("\n")}")

      logger.info("Publishing events to the event hub.")
      // Publish the events to the event hub
      val eventsToPublish = successMap.map(x =>
        new Event(
          uri = x._2._2.get._1,
          sharedAccessSignatureToken = x._2._2.get._2
        )).toList
      EventHubManager.publishEvents(
        eventHubConnectionInfo,
        eventsToPublish
      ) match {
        case Failure(exception) =>
          logger.error(s"Publishing events to the event hub failed" +
            s" with exception $exception")
          System.exit(-1)
        case Success(success) =>
          if (success)
            logger.info("Successfully published all events to the event hub")
          else
            logger.error("Failed to upload one or more events")
      }

      logger.info("Renaming files with .DONE extension.")
      listOfFiles.foreach(file => {
        val renameresult: (Boolean) = rename(
          adlsConnectionInfo,
          file
        )
        renamesuccessMap += (file -> renameresult)
      })
      if (renamesuccessMap.exists((x) => !x._2)) {
        logger.error(s"Failed renaming the following files:" +
          s" ${renamesuccessMap.filter((x) => !x._2).mkString("\n")}")
        System.exit(-1)
      } else {
        logger.info(s"Succeeded renaming the files: " +
          s" ${renamesuccessMap.keys.mkString("\n")}")
      }

    }
    logger.info(s"Completed execution")
    System.exit(0)
  }

  /**
   * Rename the file in Azure Data Lake Store
   * @param adlsConnectionInfo Azure Data Lake Store Connection Information
   * @param sourceFile File to rename
   * @return
   */
  def rename(
    adlsConnectionInfo: ADLSConnectionInfo,
    sourceFile:         String
  ): (Boolean) = {
    var success = false
    ADLSManager.renameFile(
      adlsConnectionInfo,
      sourceFile,
      ".DONE"
    ) match {
        case Success(value) => {
          logger.info(s"Successfully Renamed the file $sourceFile")
          success = true
        }
        case Failure(f) =>
          logger.error(s"failure in renaming $sourceFile" +
            s" in Azure Data Lake Store ${adlsConnectionInfo.accountFQDN}" +
            s" failed with exception $f")
      }
    (success)
  }

  /**
   * Uploads the file from Azure Data Lake Store to Azure Blob Storage
   *
   * @param adlsConnectionInfo Azure Data Lake Store Connection Information
   * @param blobConnectionInfo Azure Data Blob Store Connection Information
   * @param conf Tool configuration
   * @param sourceFile File to upload
   * @return
   */
  def upload(
    adlsConnectionInfo:     ADLSConnectionInfo,
    blobConnectionInfo:     BlobConnectionInfo,
    keyVaultConnectionInfo: KeyVaultConnectionInfo,
    conf:                   Conf,
    sourceFile:             String
  ): (Boolean, Option[(String, String)]) = {
    var success = false
    var uriAndToken: Option[(String, String)] = None

    // Generate the file name
    var blobFileName = conf.blobStoreRootFolder.getOrElse("")
    if (blobFileName.startsWith("/"))
      blobFileName = blobFileName.drop(1)
    if (blobFileName.length > 0) {
      if (!blobFileName.endsWith("/"))
        blobFileName += "/"
    }

    if (!sourceFile.startsWith("/"))
      blobFileName = s"$blobFileName$sourceFile"
    else
      blobFileName = s"$blobFileName${sourceFile.drop(1)}"

    // Get the key vault key
    val keyVaultKey = KeyVaultManager.getKey(
      keyVaultConnectionInfo,
      conf.keyVaultResourceUri()
    )

    BlobManager.getBlockBlobReference(
      blobConnectionInfo,
      conf.blobStoreContainerName(),
      blobFileName
    ) match {
        case Failure(exception) => {
          logger.error(s"Error creating block blob $sourceFile" +
            s" in container ${conf.blobStoreContainerName}" +
            s" in storage account ${blobConnectionInfo.accountName}." +
            s" Exception: $exception")
          success = false
        }
        case Success(blockBlobReference: CloudBlockBlob) => {
          def fn(stream: ADLFileInputStream) = {
            val blobEncryptionPolicy = new BlobEncryptionPolicy(keyVaultKey.get, null)
            val blobRequestOptions = new BlobRequestOptions()
            val operationContext = new OperationContext()
            blobRequestOptions.setConcurrentRequestCount(100)
            blobRequestOptions.setEncryptionPolicy(blobEncryptionPolicy)
            operationContext.setLoggingEnabled(true)
            blockBlobReference.upload(
              stream,
              -1,
              null,
              blobRequestOptions,
              operationContext
            )
          }
          ADLSManager.withAzureDataLakeStoreFileStream[Boolean](
            adlsConnectionInfo,
            sourceFile,
            fn
          ) match {
            case Failure(exception) =>
              logger.error(s"Error uploading to block blob $sourceFile" +
                s" in container ${conf.blobStoreContainerName}" +
                s" in storage account ${blobConnectionInfo.accountName}." +
                s" Exception: $exception")
              success = false
            case Success(value) =>
              if (value) {
                logger.info(s"Trying to generate shared access signature for block blob" +
                  s" $sourceFile in containers ${conf.blobStoreContainerName}")
                Try(BlobManager.getSharedAccessSignatureToken(
                  blockBlobReference,
                  conf.tokenExpiration()
                )) match {
                  case Failure(exception) =>
                    logger.error(s"Error getting a shared access signature token" +
                      s" for block blob $sourceFile in container" +
                      s" ${conf.blobStoreContainerName} in storage account " +
                      s" ${blobConnectionInfo.accountName}. Exception: $exception")
                    success = false
                  case Success(result) =>
                    uriAndToken = Some((result._1, result._2))
                    success = true
                }
              } else {
                success = false
              }
          }
        }
      }

    (success, uriAndToken)
  }

  /**
   * Return a list of files from the source
   *
   * @param conf Configuration
   * @param adlsConnectionInfo Connection Information for
   *                           Azure Data Lake Store
   */
  def getListOfFiles(
    conf:               Conf,
    adlsConnectionInfo: ADLSConnectionInfo
  ): mutable.ListBuffer[String] = {
    // Set up the source structure that this tool needs to
    // operate on
    var listOfFiles = new mutable.ListBuffer[String]()
    var sourceFolder: String = conf.sourceFolder.getOrElse("")
    if (!sourceFolder.startsWith("/")) {
      sourceFolder = s"/$sourceFolder"
    }
    if (sourceFolder.endsWith("/")) {
      sourceFolder = sourceFolder.dropRight(1)
    }

    if (conf.sourceFile.isSupplied && !conf.sourceFile().endsWith(".DONE")) {
      listOfFiles += {
        if (conf.sourceFile().startsWith("/")) {
          s"$sourceFolder${conf.sourceFile()}"
        } else {
          s"$sourceFolder/${conf.sourceFile()}"
        }
      }
    } else {
      // Based on configuration setup, we should have got
      // a folder if the file is not specified
      ADLSManager.getFilePathsInFolder(
        adlsConnectionInfo,
        sourceFolder
      ) match {
        case Success(lf) =>
          lf.foreach(s => listOfFiles += {
            if (s.startsWith("/"))
              s"$sourceFolder$s"
            else
              s"$sourceFolder/$s"
          })
        case Failure(f) =>
          logger.error(s"Getting list of files from $sourceFolder" +
            s" in Azure Data Lake Store ${adlsConnectionInfo.accountFQDN}" +
            s" failed with exception $f")
      }
    }
    listOfFiles
  }
}
