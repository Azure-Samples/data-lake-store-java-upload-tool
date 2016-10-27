package com.microsoft.azure.adls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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

  /**
   * Constructor that initializes the executor services for the ADLS file uploader.
   *
   * @param parallelism Parallelism level
   */
  AzureDataLakeStoreUploader(int parallelism) {
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
    logger.info("Uploading {} to Azure", path);
    return path;
  }

  /**
   * Orchestrates the upload to Azure Data Lake Storage.
   *
   * @param inputPathStream Path stream of source file that qualify for upload
   */
  void upload(Stream<Path> inputPathStream) {
    Stream<CompletableFuture<Path>> completableFutureStream = inputPathStream.map(
        sourcePath ->
            CompletableFuture.supplyAsync(() ->
                this.setStatusToInProgress(sourcePath), this.executorServicePool))
        .map(pathToUpload -> pathToUpload.thenApply(
            this::uploadToAzureDataLakeStore))
        .map(inProgressPath -> inProgressPath.thenApply(
            this::setStatusToCompleted));
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
        logger.info("Attempting graceful shutdown of the executor service.", executorServicePool);
        executorServicePool.shutdown();
        executorServicePool.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error(
            "Graceful shutdown of the executor service did not complete within {} seconds.",
            timeoutInSeconds);
      } finally {
        if (!executorServicePool.isTerminated()) {
          logger.info("Forcing shutdown of the executor service.");
          executorServicePool.shutdownNow();
        }
        logger.info("Shutdown of the executor service completed.");
      }
    } else {
      logger.info("Executor service has already shutdown.");
    }
  }
}
