package com.microsoft.azure.adls;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * AzureDataLakeStoreUploader is responsible for uploading the stream of local files
 * to Azure Data Lake. This class uses executor service to manage
 * parallelism.
 *
 * @author Gandhinath Swaminathan
 */
class AzureDataLakeStoreUploader {
  private static final Logger logger = LoggerFactory.getLogger(
      AzureDataLakeStoreUploader.class.getName());

  private final ExecutorService executorServicePool;
  private final Path sourceRoot;
  private final String clientId;
  private final String authTokenEndpoint;
  private final String clientKey;
  private final String accountFqdn;
  private final String destinationRoot;

  /**
   * Constructor that initializes the executor services for the ADLS file uploader.
   *
   * @param sourceRoot        Root path of the source folder. Used to compute the path
   *                          in Azure Data Lake Storage
   * @param clientId          Client Id of the Azure active directory application
   * @param authTokenEndpoint Authentication Token Endpoint of the Azure active
   *                          directory application
   * @param clientKey         Client key for the Azure active directory application
   * @param accountFqdn       Fully Qualified Domain Name of the Azure data lake account.
   * @param destinationRoot   Root of the ADLS folder path into which the files will be uploaded
   * @param parallelism       Parallelism level
   */
  AzureDataLakeStoreUploader(Path sourceRoot,
                             String clientId,
                             String authTokenEndpoint,
                             String clientKey,
                             String accountFqdn,
                             String destinationRoot,
                             int parallelism) {
    this.sourceRoot = sourceRoot;
    this.clientId = clientId;
    this.authTokenEndpoint = authTokenEndpoint;
    this.clientKey = clientKey;
    this.accountFqdn = accountFqdn;
    this.destinationRoot = destinationRoot;
    executorServicePool = Executors.newWorkStealingPool(parallelism);
  }

  /**
   * Moves the file to in progress status by renaming the file.
   *
   * @param path Source path
   * @return Target path
   */
  private Path setStatusToInProgress(Path path) {
    Path targetPath = Paths.get(
        path
            .toString()
            .concat(FolderUtils.INPROGRESS_EXTENSION));

    FolderUtils.move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }

  /**
   * Moves the file to completed status by renaming the file.
   *
   * @param path Source path
   * @return Target path
   */
  private Path setStatusToCompleted(Path path) {
    String sourcePathString = path.toString();
    String targetPathString;

    // Handle in progress or fresh files
    if (sourcePathString.endsWith(FolderUtils.INPROGRESS_EXTENSION)) {
      targetPathString = sourcePathString.replace(
          FolderUtils.INPROGRESS_EXTENSION,
          FolderUtils.COMPLETED_EXTENSION);
    } else {
      targetPathString = sourcePathString.concat(FolderUtils.COMPLETED_EXTENSION);
    }

    Path targetPath = Paths.get(targetPathString);
    FolderUtils.move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }

  /**
   * Uploads the local file to Azure Data Lake Store.
   *
   * @param path Source path
   * @return Source path
   */
  private Path uploadToAzureDataLakeStore(Path path) {
    ADLStoreClient client = null;

    // Obtain OAuth2 token and use token to create client object
    try {
      AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(
          this.authTokenEndpoint,
          this.clientId,
          this.clientKey);
      client = ADLStoreClient.createClient(
          this.accountFqdn,
          token);
    } catch (ADLException ex) {
      logger.error(
          "Acquiring Azure AD token and instantiating ADLS client failed with exception: {}",
          ex.getMessage());
    } catch (Exception ex) {
      logger.error(
          "Acquiring Azure AD token and instantiating ADLS client failed with an unknown exception: {}",
          ex.getMessage());
    }

    if (client != null) {
      String fileName = path.getFileName().toString();
      // Destination file should not have the in progress extension
      if (fileName.endsWith(FolderUtils.INPROGRESS_EXTENSION)) {
        fileName = fileName.replace(FolderUtils.INPROGRESS_EXTENSION, "");
      }

      // Algorithm to figure out the destination path
      // Takes the source root and remove that from the full path
      // This gives us the file name and the path we have to actually create
      // on the ADLS. Then concatenate the destination root with the remaining file
      // path to create the destination folder.
      String destination = destinationRoot;
      if (destination.endsWith("/")) {
        destination = destination.substring(0, destination.length() - 1);
      }
      String remainingPath = path
          .toString()
          .replace(sourceRoot.toString(), "")
          .replace(path.getFileName().toString(), "");
      destination = destination
          .concat(remainingPath);

      // Try uploading the file
      try {
        logger.debug("Initiating upload of {} to {}",
            path.toString(),
            destination);

        OutputStream stream = client.createFile(
            filename,
            IfExists.OVERWRITE,
            "744",
            true);

        logger.debug("Completing upload of {} to {}",
            path.toString(),
            destination);
      } catch (ADLException ex) {
        logger.error(
            "Uploading {} to {} failed with exception: {}",
            sourceRoot,
            destination,
            ex.getMessage());
      } catch (Exception ex) {
        logger.error(
            "Uploading {} to {} failed with an unknown exception: {}",
            sourceRoot,
            destination,
            ex.getMessage());
      }
    } else {
      logger.warn("ADLS client is not initialized. Ignoring {}", path.toString());
    }

    return path;
  }

  /**
   * Orchestrates the upload to Azure Data Lake Storage.
   *
   * @param inputPathList List of source file path that qualify for upload
   */
  void upload(List<Path> inputPathList) {
    // ThenApply ensures that the code executes on the same
    // thread as it's predecessor
    List<CompletableFuture<Path>> completableFutureList = inputPathList.stream().map(
        sourcePath ->
            CompletableFuture.supplyAsync(() ->
                this.setStatusToInProgress(sourcePath), this.executorServicePool)
                .thenApply(this::uploadToAzureDataLakeStore)
                .thenApply(this::setStatusToCompleted))
        .collect(toList());

    // Wait for all the futures to be complete
    // and translate the result into an array
    CompletableFuture<Void> allDoneFuture =
        CompletableFuture.allOf(
            completableFutureList.toArray(
                new CompletableFuture[completableFutureList.size()]));

    allDoneFuture.thenApply(v ->
        completableFutureList.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList()));
  }

  /**
   * Gracefully terminate the executor service.
   *
   * @param timeoutInSeconds Time to wait in seconds before forcefully
   *                         shutting down the executor service
   */
  void terminate(long timeoutInSeconds) {
    if (!executorServicePool.isShutdown() && !executorServicePool.isTerminated()) {
      try {
        logger.debug("Attempting graceful shutdown of the executor service.", executorServicePool);
        executorServicePool.shutdown();
        executorServicePool.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error(
            "Graceful shutdown of the executor service did not complete within {} seconds.",
            timeoutInSeconds);
      } finally {
        if (!executorServicePool.isTerminated()) {
          logger.debug("Forcing shutdown of the executor service.");
          executorServicePool.shutdownNow();
        }
        logger.debug("Shutdown of the executor service completed.");
      }
    } else {
      logger.debug("Executor service has already shutdown.");
    }
  }
}
