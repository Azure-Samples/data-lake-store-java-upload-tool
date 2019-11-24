package com.starbucks.analytics.blob

import java.util

import com.microsoft.azure.storage.blob._
import com.microsoft.azure.storage.{CloudStorageAccount, OperationContext}
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.Try

/**
 * Manages all the Azure Blob Store operations
 */
object BlobManager {
  private val logger = Logger("BlobManager")

  /**
   * Loan a Azure Blob Store client
   *
   * @param connectionInfo Azure Data Blob Store Connection Information
   * @param f              Function that takes the Azure Data Lake Store
   *                       connection and returns the result of type R
   * @tparam R Type of the return value
   * @return Return value
   */
  private def withAzureBlobStoreClient[R](
    connectionInfo: BlobConnectionInfo,
    f:              (CloudBlobClient) => R
  ): Try[R] = {
    val storageConnectionString = s"DefaultEndpointsProtocol=https;" +
      s"AccountName=${connectionInfo.accountName};" +
      s"AccountKey=${connectionInfo.accountKey};" +
      s"EndpointSuffix = core.windows.net"

    val account = CloudStorageAccount.parse(storageConnectionString)
    val serviceClient = account.createCloudBlobClient()
    val result = Try(f(serviceClient))
    result
  }

  /**
   * Loan a Azure Blob Storage Container
   *
   * @param connectionInfo Azure Data Blob Store Connection Information
   * @param containerName Name of the container
   *
   * @return Reference to the cloud blob container
   */
  private def withAzureBlobContainer[R](
    connectionInfo: BlobConnectionInfo,
    containerName:  String,
    f:              (CloudBlobContainer) => R
  ): Try[R] = {
    def fn(serviceClient: CloudBlobClient): R = {
      val container = serviceClient.getContainerReference(containerName)
      val blobRequestOptions = new BlobRequestOptions()
      val operationContext = new OperationContext()
      blobRequestOptions.setConcurrentRequestCount(100)
      operationContext.setLoggingEnabled(true)
      container.createIfNotExists(
        BlobContainerPublicAccessType.OFF,
        blobRequestOptions,
        operationContext
      )
      f(container)
    }
    withAzureBlobStoreClient(
      connectionInfo,
      fn
    )
  }

  /**
   * Creates a reference to the block blob within
   * the specific container
   *
   * @param connectionInfo Azure Data Blob Store Connection Information
   * @param containerName Name of the container
   * @param blobName Name of the blob within the container
   * @return
   */
  def getBlockBlobReference(
    connectionInfo: BlobConnectionInfo,
    containerName:  String,
    blobName:       String
  ): Try[CloudBlockBlob] = {
    def fn(container: CloudBlobContainer): CloudBlockBlob = {
      container.getBlockBlobReference(blobName)
    }
    withAzureBlobContainer[CloudBlockBlob](
      connectionInfo,
      containerName,
      fn
    )
  }

  /**
   * Gets a shared access signature token for the block blob specified
   *
   * @param blockBlobReference Reference to the block blob
   * @param tokenExpirationInMinutes Token expiration time in minutes
   * @return Token
   */
  def getSharedAccessSignatureToken(
    blockBlobReference:       CloudBlockBlob,
    tokenExpirationInMinutes: Int
  ): (String, String) = {
    // Set the expiry time and permissions for the blob.
    // In this case, the start time is specified as a few minutes in the past, to mitigate clock skew.
    // The shared access signature will be valid immediately.
    val sasConstraints = new SharedAccessBlobPolicy()
    sasConstraints.setSharedAccessStartTime(
      new DateTime(DateTimeZone.UTC)
      .plusMinutes(-5)
      .toDate
    )
    sasConstraints.setSharedAccessExpiryTime(
      new DateTime(DateTimeZone.UTC)
      .plusMinutes(tokenExpirationInMinutes)
      .toDate
    )
    sasConstraints.setPermissions(util.EnumSet.of(SharedAccessBlobPermissions.READ))
    val sasToken: String = blockBlobReference.generateSharedAccessSignature(
      sasConstraints,
      null
    )
    (blockBlobReference.getStorageUri.toString, sasToken)
  }

}
