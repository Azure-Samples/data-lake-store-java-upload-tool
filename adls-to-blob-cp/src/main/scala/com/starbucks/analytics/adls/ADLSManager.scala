package com.starbucks.analytics.adls

import com.microsoft.azure.datalake.store.oauth2.{ AccessTokenProvider, ClientCredsTokenProvider }
import com.microsoft.azure.datalake.store.{ ADLFileInputStream, ADLStoreClient, DirectoryEntryType }
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

/**
 * Manages all the Azure Data Lake Store operations
 */
object ADLSManager {
  private val logger = Logger("ADLSManager")

  /**
   * Returns an azure active directory token using the application credentials you created.
   *
   * @param clientId                    Client Id of the application you registered with active directory
   * @param clientKey                   Client key of the application you registered with active directory
   * @param authenticationTokenEndpoint OAuth2 endpoint for the application
   *                                    you registered with active directory
   * @return Azure AD Token
   */
  private def getAzureADTokenProvider(
    clientId:                    String,
    clientKey:                   String,
    authenticationTokenEndpoint: String
  ): AccessTokenProvider = {
    val tokenProvider = new ClientCredsTokenProvider(
      authenticationTokenEndpoint,
      clientId,
      clientKey
    )
    tokenProvider
  }

  /**
   * Returns a client to connect to Azure Data Lake Store.
   *
   * @param accountFQDN  Fully Qualified Domain Name of the Azure data lake store
   * @param azureADTokenProvider Azure AD Token Provider . You can use getAzureADToken to get a valid token.
   * @return Azure Data Lake Store client
   */
  private def getAzureDataLakeStoreClient(
    accountFQDN:          String,
    azureADTokenProvider: AccessTokenProvider
  ): ADLStoreClient = {
    ADLStoreClient.createClient(
      accountFQDN,
      azureADTokenProvider
    )
  }

  /**
   * Loan a Azure Data Lake Store client
   *
   * @param connectionInfo Azure Data Lake Store Connection Information
   * @param f              Function that takes the Azure Data Lake Store
   *                       connection and returns the result of type R
   * @tparam R Type of the return value
   * @return Return value
   */
  private def withAzureDataLakeStoreClient[R](
    connectionInfo: ADLSConnectionInfo,
    f:              (ADLStoreClient) => R
  ): Try[R] = {
    val adlStoreClient = getAzureDataLakeStoreClient(
      connectionInfo.accountFQDN,
      getAzureADTokenProvider(
        connectionInfo.clientId,
        connectionInfo.clientKey,
        connectionInfo.authenticationTokenEndpoint
      )
    )
    val result = Try(f(adlStoreClient))
    result
  }

  /**
   * Checks if the folder or file exists in the Azure Data Lake
   *
   * @param connectionInfo Azure Data Lake Store Connection Information
   * @param path Path to clean up
   */
  private def checkIfPathExists(
    connectionInfo: ADLSConnectionInfo,
    path:           String
  ): Try[Boolean] = {
    def fn(adlStoreClient: ADLStoreClient): Boolean = {
      logger.info(s"Checking for existance of path $path" +
        s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
      adlStoreClient.checkExists(path)
    }
    withAzureDataLakeStoreClient(
      connectionInfo,
      fn
    )
  }

  /**
   * Get a list of file paths in the specified folder in the Azure Data Lake
   *
   * @param connectionInfo Azure Data Lake Store Connection Information
   * @param path Path to clean up
   */
  def getFilePathsInFolder(
    connectionInfo: ADLSConnectionInfo,
    path:           String
  ): Try[List[String]] = {
    def fn(adlStoreClient: ADLStoreClient): List[String] = {
      adlStoreClient.enumerateDirectory(path)
        .asScala
        .filter(di => di.`type` == DirectoryEntryType.FILE)
        .filter(di => di.name.substring((di.name).length() - 5) != ".DONE")
        .map(di => di.name)
        .toList
    }
    checkIfPathExists(connectionInfo, path) match {
      case Success(exists) =>
        if (exists) {
          logger.info(s"Getting files in path $path" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
          withAzureDataLakeStoreClient(
            connectionInfo,
            fn
          )
        } else {
          logger.info(s"Path $path does not exist" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
          Try(List[String]())
        }
      case Failure(f) =>
        logger.error(s"Getting files in path $path" +
          s" in Azure Data Lake Store ${connectionInfo.accountFQDN}" +
          s" failed with exception $f")
        Try(List[String]())
    }
  }

  /**
   * Deletes the folders and the files recursively in the specified
   * Azure Data Lake path
   *
   * @param connectionInfo Azure Data Lake Store Connection Information
   * @param path Path to clean up
   */
  def deletePath(
    connectionInfo: ADLSConnectionInfo,
    path:           String
  ): Try[Boolean] = {
    def fn(adlStoreClient: ADLStoreClient): Boolean = {
      adlStoreClient.deleteRecursive(path)
    }
    checkIfPathExists(connectionInfo, path) match {
      case Success(exists) =>
        if (exists) {
          logger.info(s"Deleting path $path" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
          withAzureDataLakeStoreClient(
            connectionInfo,
            fn
          )
        } else {
          logger.info(s"Path $path does not exist" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
          Try(false)
        }
      case Failure(f) =>
        logger.error(s"Deleting path $path" +
          s" in Azure Data Lake Store ${connectionInfo.accountFQDN}" +
          s" failed with exception $f")
        Try(false)
    }
  }

  /**
   * Opens an input stream for the specified file
   *
   * @param connectionInfo Azure Data Lake Store Connection Information
   * @param path Path to clean up
   * @param f Function that takes the Azure Data Lake Store Input file
   *                       stream and returns the result of type R
   * @tparam R Type of the return value
   * @return Return value
   */
  def withAzureDataLakeStoreFileStream[R](
    connectionInfo: ADLSConnectionInfo,
    path:           String,
    f:              (ADLFileInputStream) => Unit
  ): Try[Boolean] = {
    def fn(adlStoreClient: ADLStoreClient): Boolean = {
      var readStream: Option[ADLFileInputStream] = None
      var succeeded: Boolean = false
      try {
        readStream = Some(adlStoreClient.getReadStream(path))
        f(readStream.get)
        succeeded = true
      } catch {
        case e: Exception => {
          logger.error(s"Reading from path $path" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}" +
            s" failed with exception $e")
          succeeded = false
        }
      } finally {
        if (readStream.isDefined) {
          readStream.get.close()
        }
      }
      succeeded
    }
    // Assume that the caller has validated existence of
    // this path
    withAzureDataLakeStoreClient(
      connectionInfo,
      fn
    )
  }

  /**
   * Renames the file in the specified by adding the specified extension.
   * Azure Data Lake path
   *
   * @param connectionInfo Azure Data Lake Store Connection Information
   * @param path Path to the file
   * @param extension to add to the filename, include the period (.)
   */
  def renameFile(
    connectionInfo: ADLSConnectionInfo,
    path:           String,
    extension:      String
  ): Try[Boolean] = {
    def fn(adlStoreClient: ADLStoreClient): Boolean = {
      adlStoreClient.rename(path, path + extension, true)
    }
    checkIfPathExists(connectionInfo, path) match {
      case Success(exists) =>
        if (exists) {
          logger.info(s"Renaming the file $path" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
          withAzureDataLakeStoreClient(
            connectionInfo,
            fn
          )
        } else {
          logger.info(s"File $path does not exist" +
            s" in Azure Data Lake Store ${connectionInfo.accountFQDN}")
          Try(false)
        }
      case Failure(f) =>
        logger.error(s"Renaming file $path" +
          s" in Azure Data Lake Store ${connectionInfo.accountFQDN}" +
          s" failed with exception $f")
        Try(false)
    }
  }
}
