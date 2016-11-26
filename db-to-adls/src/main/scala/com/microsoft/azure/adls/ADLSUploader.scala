package com.microsoft.azure.adls

import com.microsoft.azure.adls.db.PartitionMetadata
import com.microsoft.azure.datalake.store.oauth2.{AzureADAuthenticator, AzureADToken}
import com.microsoft.azure.datalake.store.{ADLFileOutputStream, ADLStoreClient, IfExists}

import scala.collection.mutable

/**
  * Manages uploads of files to the Azure Data Lake Store.
  *
  * @param adlStoreClient           Connected client to ADL Store
  * @param path                     Specific path of the file
  * @param octalPermissions         Permissions on the file
  * @param desiredBufferSizeInBytes Desired buffer size to hold the data buffers until it is uploaded
  */
class ADLSUploader(adlStoreClient: ADLStoreClient,
                   path: String,
                   octalPermissions: String,
                   desiredBufferSizeInBytes: Int) extends AutoCloseable {
  private lazy val stream: ADLFileOutputStream = adlStoreClient.createFile(path,
    IfExists.OVERWRITE,
    octalPermissions,
    true)
  stream.setBufferSize(desiredBufferSizeInBytes)
  private var currentBufferSize: Int = 0
  private var bufferBuilder = new mutable.ArrayBuilder.ofByte()

  /**
    * Buffers the data until it reaches the threshold
    *
    * @param data Byte buffer to write to the stream
    */
  def bufferedUpload(data: Array[Byte]): Unit = {
    if ((currentBufferSize + data.length) >= desiredBufferSizeInBytes) {
      bufferBuilder ++= data
      currentBufferSize += data.length
    } else {
      upload(bufferBuilder.result())
      bufferBuilder.clear()
      bufferBuilder ++= data
      currentBufferSize = data.length
    }
  }

  /**
    * AutoCloseable to ensure resources are released appropriately
    */
  override def close(): Unit = {
    upload(bufferBuilder.result())

    if (stream != null) {
      stream.flush()
      stream.close()
    }
  }

  /**
    * Writes raw buffer to the stream
    *
    * @param data Byte buffer to write to the stream
    */
  private def upload(data: Array[Byte]) = {
    stream.write(data)
  }


}

/**
  * Companion Object
  */
object ADLSUploader {
  /**
    * Returns an azure active directory token using the application credentials you created.
    *
    * @param clientId                    Client Id of the application you registered with active directory
    * @param clientKey                   Client key of the application you registered with active directory
    * @param authenticationTokenEndpoint OAuth2 endpoint for the application
    *                                    you registered with active directory
    * @return Azure AD Token
    */
  private def getAzureADToken(clientId: String,
                              clientKey: String,
                              authenticationTokenEndpoint: String): AzureADToken = {
    AzureADAuthenticator.getTokenUsingClientCreds(
      authenticationTokenEndpoint,
      clientId,
      clientKey)
  }

  /**
    * Returns a client to connect to Azure Data Lake Store.
    *
    * @param accountFQDN  Fully Qualified Domain Name of the Azure data lake store
    * @param azureADToken Azure AD Token. You can use getAzureADToken to get a valid token.
    * @return Azure Data Lake Store client
    */
  private def getAzureDataLakeStoreClient(accountFQDN: String,
                                          azureADToken: AzureADToken): ADLStoreClient = {
    ADLStoreClient.createClient(
      accountFQDN,
      azureADToken)
  }

  /**
    * Generate ADLS Path given the partition metadata
    *
    * @param destination       Root folder structure
    * @param partitionMetadata Partition structure in the database backend
    * @return Full Path of ADLS file
    */
  def getADLSPath(destination: String,
                  partitionMetadata: PartitionMetadata): String = {
    val fullPath: StringBuilder = new StringBuilder
    val fileName: StringBuilder = new StringBuilder
    fullPath ++= s"$destination/"
    fullPath ++= s"${partitionMetadata.tableName}/"
    if (partitionMetadata.partitionName.isDefined) {
      fullPath ++= s"${partitionMetadata.partitionName.get}/"
      fileName ++= s"${partitionMetadata.partitionName.get}_"
    }
    if (partitionMetadata.subPartitionName.isDefined) {
      fullPath ++= s"${partitionMetadata.subPartitionName.get}/"
      fileName ++= s"${partitionMetadata.subPartitionName.get}_"
    }
    fileName ++= s"${partitionMetadata.tableName}.tsv"
    fullPath ++= fileName

    fullPath.toString()
  }

  /**
    * Creates a new instance of the ADLSUploader
    *
    * @param clientId                    Client Id of the application you registered with active directory
    * @param clientKey                   Client key of the application you registered with active directory
    * @param authenticationTokenEndpoint OAuth2 endpoint for the application
    *                                    you registered with active directory
    * @param accountFQDN                 Fully Qualified Domain Name of the Azure data lake store
    * @param path                        Specific path of the file
    * @param octalPermissions            Permissions on the file
    * @param desiredBufferSizeInBytes    Desired buffer size to hold the data buffers until it is uploaded
    * @return
    */
  def apply(clientId: String,
            clientKey: String,
            authenticationTokenEndpoint: String,
            accountFQDN: String,
            path: String,
            octalPermissions: String,
            desiredBufferSizeInBytes: Int): ADLSUploader = {
    // You can abstract the store client outside of this class.
    // However, the tokens expire after an hour and forces renewal.
    // Better to abstract this per upload to avoid exceptions and reduce complexity.
    val adlStoreClient: ADLStoreClient = getAzureDataLakeStoreClient(accountFQDN,
      getAzureADToken(clientId,
        clientKey,
        authenticationTokenEndpoint))

    new ADLSUploader(adlStoreClient, path, octalPermissions, desiredBufferSizeInBytes)
  }
}