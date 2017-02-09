package com.starbucks.analytics.adls

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ ExecutorService, Executors, LinkedBlockingQueue, TimeUnit }

import com.microsoft.azure.datalake.store.oauth2.{ AccessTokenProvider, ClientCredsTokenProvider }
import com.microsoft.azure.datalake.store.{ ADLFileOutputStream, ADLStoreClient, IfExists }
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{ Failure, Success, Try }

/**
 * Manages uploads of files to the Azure Data Lake Store.
 *
 * @param adlStoreClient           Connected client to ADL Store
 * @param path                     Specific path of the file
 * @param octalPermissions         Permissions on the file
 * @param desiredBufferSizeInBytes Desired buffer size to hold the data buffers until it is uploaded
 */
class ADLSUploader(
    adlStoreClient:           ADLStoreClient,
    path:                     String,
    octalPermissions:         String,
    desiredBufferSizeInBytes: Int
) extends AutoCloseable {
  private val regex: Regex = """-""".r
  private val conformedPath: String = regex.replaceAllIn(path, "_")
  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[ADLSUploader])
  private lazy val stream: ADLFileOutputStream = adlStoreClient.createFile(
    conformedPath,
    IfExists.OVERWRITE,
    octalPermissions,
    true
  )
  private val queue: LinkedBlockingQueue[Array[Byte]] = new LinkedBlockingQueue[Array[Byte]]()
  private val executorService: ExecutorService = Executors.newSingleThreadExecutor()
  private val shouldContinueUploading: AtomicBoolean = new AtomicBoolean(true)
  private val bufferBuilder = mutable.ArrayBuilder.make[Byte]
  private var currentBufferSize: Int = 0

  stream.setBufferSize(desiredBufferSizeInBytes)
  // Note: If the capacity is the same as size, the array builder
  // will not copy; rather give the reference. to avoid this
  // situation, add two more bytes to the capacity
  bufferBuilder.sizeHint(desiredBufferSizeInBytes + 2)

  // Blocking thread to process the upload queue
  executorService.execute(new Runnable {
    override def run(): Unit = {
      var terminate: Boolean = false
      while (shouldContinueUploading.get() && !terminate) {
        Option(queue.poll(200, TimeUnit.MILLISECONDS)) match {
          case Some(Array.emptyByteArray) =>
            logger.debug(
              s"""
                |Empty array received. Sending terminate signal.
              """.stripMargin
            )
            terminate = true
          case (Some(data)) =>
            upload(data)
          case None => // Nothing to do
        }
      }
    }
  })

  /**
   * Buffers the data until it reaches the threshold
   *
   * @param data Byte buffer to write to the stream
   */
  def bufferedUpload(data: Array[Byte]): Unit = {
    if (!shouldContinueUploading.get()) {
      throw new Exception("There was an exception uploading to Azure. Please check your logs.")
    }
    if (desiredBufferSizeInBytes >= (currentBufferSize + data.length)) {
      bufferBuilder ++= data
      currentBufferSize += data.length
    } else {
      queue.put(bufferBuilder.result)
      bufferBuilder.clear()
      bufferBuilder.sizeHint(desiredBufferSizeInBytes + 2)
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
    val result = Try(stream.write(data))
    result match {
      case Success(_: Unit) =>
        shouldContinueUploading.set(true)
      case Failure(error: Throwable) =>
        shouldContinueUploading.set(false)
        logger.error(s"Error uploading byte array to Azure Data Lake: $path", error)
    }
  }

  /**
   * AutoCloseable to ensure resources are released appropriately
   */
  override def close(): Unit = {
    // Wait for uploader thread to complete
    if (shouldContinueUploading.get()) {
      queue.put(bufferBuilder.result)
      queue.put(Array.emptyByteArray)
      logger.info("Waiting for uploader threads to complete")
      executorService.shutdown()
      Try(executorService.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS))
    } else {
      logger.info("There was a failure during upload. Shutting down abruptly")
      executorService.shutdownNow()
    }
    // Shutdown the stream
    if (stream != null) {
      stream.flush()
      stream.close()
    }
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
   * @return New instance of the uploader
   */
  def apply(
    clientId:                    String,
    clientKey:                   String,
    authenticationTokenEndpoint: String,
    accountFQDN:                 String,
    path:                        String,
    octalPermissions:            String,
    desiredBufferSizeInBytes:    Int
  ): ADLSUploader = {
    // You can abstract the store client outside of this class.
    // However, the tokens expire after an hour and forces renewal.
    // Better to abstract this per upload to avoid exceptions and reduce complexity.
    val adlStoreClient: ADLStoreClient = getAzureDataLakeStoreClient(
      accountFQDN,
      getAzureADTokenProvider(
        clientId,
        clientKey,
        authenticationTokenEndpoint
      )
    )

    new ADLSUploader(adlStoreClient, path, octalPermissions, desiredBufferSizeInBytes)
  }
}