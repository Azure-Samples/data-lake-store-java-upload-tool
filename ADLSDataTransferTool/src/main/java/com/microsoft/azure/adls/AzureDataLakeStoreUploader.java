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
   * A completable future that moves the file to
   * in progress status by renaming the file.
   *
   * @param path                Source path
   * @param executorServicePool Pool used by the completable future
   * @return Target path
   */
  private static CompletableFuture<Path> setStatusToInProgress(
      Path path,
      ExecutorService executorServicePool) {
    return CompletableFuture.supplyAsync(() -> {
      Path targetPath = Paths.get(
          path
              .toString()
              .concat(FolderUtils.INPROGRESS_EXTENSION));

      FolderUtils.move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
      return targetPath;
    }, executorServicePool);
  }

  /**
   * A completable future that moves the file to
   * completed status by renaming the file.
   *
   * @param path                Source path
   * @param executorServicePool Pool used by the completable future
   * @return Target path
   */
  private static CompletableFuture<Path> setStatusToCompleted(
      Path path,
      ExecutorService executorServicePool) {
    return CompletableFuture.supplyAsync(() -> {
      String sourcePathString = path.toString();
      String targetPathString = "";
      if (sourcePathString.contains(FolderUtils.INPROGRESS_EXTENSION)) {
        targetPathString = sourcePathString.replace(
            FolderUtils.INPROGRESS_EXTENSION,
            FolderUtils.COMPLETED_EXTENSION);
      } else {
        targetPathString = sourcePathString.concat(FolderUtils.COMPLETED_EXTENSION);
      }
      Path targetPath = Paths.get(targetPathString);
      FolderUtils.move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
      return targetPath;
    }, executorServicePool);
  }

  /**
   * A completable future that moves the local file to Azure Data Lake Store.
   *
   * @param path                Source path
   * @param executorServicePool Pool used by the completable future
   * @return Source path
   */
  private static CompletableFuture<Path> uploadToAzureDataLakeStore(
      Path path,
      ExecutorService executorServicePool) {
    return CompletableFuture.supplyAsync(() -> {
      logger.info("Uploading {} to Azure", path);
      return path;
    }, executorServicePool);
  }

  /**
   * Orchestrates the upload to Azure Data Lake Storage.
   *
   * @param inputPathStream Path stream of source file that qualify for upload
   */
  void upload(Stream<Path> inputPathStream) {
    inputPathStream.map(
        sourcePath -> setStatusToInProgress(sourcePath, this.executorServicePool)
            .thenApply(pathToUpload -> uploadToAzureDataLakeStore(
                pathToUpload,
                this.executorServicePool))
            .thenApply(inProgressPath -> setStatusToCompleted(
                inProgressPath,
                this.executorServicePool)));
//    inputPathStream
//        .map(sourcePath -> setStatusToInProgress(sourcePath, this.executorServicePool))
//        .map(pathToUpload -> pathToUpload.thenApply(path ->
//            uploadToAzureDataLakeStore(
//                path,
//                this.executorServicePool)))
//        .map(inProgressPath -> inProgressPath.thenApply(path ->
//            setStatusToCompleted(
//                path,
//                this.executorServicePool)));
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
