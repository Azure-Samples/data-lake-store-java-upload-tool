package com.microsoft.azure.adls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

  private final ExecutorService executorService;

  /**
   * Constructor that initializes the executor services for the ADLS file uploader.
   *
   * @param parallelism Parallelism level
   */
  AzureDataLakeStoreUploader(int parallelism) {
    executorService = Executors.newWorkStealingPool(parallelism);
  }

  /**
   * Gracefully terminate the executor service.
   *
   * @param timeoutInSeconds Time to wait in seconds before forcefully
   *                         shutting down the executor service
   */
  void terminate(long timeoutInSeconds) {
    if (!executorService.isShutdown() && !executorService.isTerminated()) {
      try {
        logger.info("Attempting graceful shutdown of the executor service.", executorService);
        executorService.shutdown();
        executorService.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error(
            "Graceful shutdown of the executor service did not complete within {} seconds.",
            timeoutInSeconds);
      } finally {
        if (!executorService.isTerminated()) {
          logger.info("Forcing shutdown of the executor service.");
          executorService.shutdownNow();
        }
        logger.info("Shutdown of the executor service completed.");
      }
    } else {
      logger.info("Executor service has already shutdown.");
    }
  }
}
