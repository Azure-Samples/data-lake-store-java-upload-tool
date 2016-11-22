package com.microsoft.azure.adls

import java.io.OutputStream

import com.microsoft.azure.datalake.store.oauth2.{AzureADAuthenticator, AzureADToken}
import com.microsoft.azure.datalake.store.{ADLStoreClient, IfExists}

import scala.collection.mutable

/**
  * Manages uploads of files to the Azure Data Lake Store.
  *
  * @param clientId                    Client Id of the application you registered with active directory
  * @param clientKey                   Client key of the application you registered with active directory
  * @param authenticationTokenEndpoint OAuth2 endpoint for the application
  *                                    you registered with active directory
  * @param accountFQDN                 Fully Qualified Domain Name of the Azure data lake store
  * @param path                        Specific path of the file
  * @param octalPermissions            Permissions on the file
  */
class ADLSUploader(clientId: String,
                   clientKey: String,
                   authenticationTokenEndpoint: String,
                   accountFQDN: String,
                   path: String,
                   octalPermissions: String,
                   desiredBufferSizeInBytes: Int) extends AutoCloseable {

  // You can abstract the store client outside of this class.
  // However, the tokens expire after an hour and forces renewal.
  // Better to abstract this per upload to avoid exceptions and reduce complexity.
  lazy val adlStoreClient = getAzureDataLakeStoreClient(accountFQDN,
    getAzureADToken(clientId,
      clientKey,
      authenticationTokenEndpoint))
  lazy val stream: OutputStream = adlStoreClient.createFile(path,
    IfExists.OVERWRITE,
    octalPermissions,
    true)
  var currentBufferSize: Int = 0
  var bufferBuilder = new mutable.ArrayBuilder.ofByte()

  /**
    * Buffers the data until it reaches the threshold
    *
    * @param data Byte buffer to write to the stream
    */
  def bufferedUpload(data: Array[Byte]) = {
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
    * Writes raw buffer to the stream
    *
    * @param data Byte buffer to write to the stream
    */
  private def upload(data: Array[Byte]) = {
    stream.write(data)
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
    * Returns an azure active directory token using the application credentials you created.
    *
    * @param clientId                    Client Id of the application you registered with active directory
    * @param clientKey                   Client key of the application you registered with active directory
    * @param authenticationTokenEndpoint OAuth2 endpoint for the application
    *                                    you registered with active directory
    * @return Azure AD Token
    */
  def getAzureADToken(clientId: String,
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
  def getAzureDataLakeStoreClient(accountFQDN: String,
                                  azureADToken: AzureADToken): ADLStoreClient = {
    ADLStoreClient.createClient(
      accountFQDN,
      azureADToken)
  }
}
