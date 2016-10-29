package com.microsoft.azure.adls;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

  /**
   * Entry point for the application.
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    // Parse the command line arguments
    // prepare for execution
    Cli cli = new Cli(args);
    if (cli.parse()) {
      CommandLine commandLine = cli.getCommandLine();
      Option[] options = commandLine.getOptions();
      if (options.length > 0) {
        logger.info("Starting {} with the following command line arguments", Cli.APPLICATION);
        for (Option option : options) {
          logger.info("{} ({}): {}",
              option.getLongOpt(),
              option.getDescription(),
              option.hasArg() ? commandLine.getOptionValue(option.getOpt()) : "TRUE");
        }

        // Prepare for execution
        String wildCard = "**/*";
        int desiredParalellism = Runtime.getRuntime().availableProcessors();
        int desiredBufferSize = 256 * 1024 * 1024;  // 256 MB by default
        if (commandLine.hasOption(Cli.WILDCARD)) {
          wildCard = commandLine.getOptionValue(Cli.WILDCARD);
        }
        if (commandLine.hasOption(Cli.DESIRED_PARALLELISM)) {
          desiredParalellism = Integer.parseInt(
              commandLine.getOptionValue(Cli.DESIRED_PARALLELISM));
        }
        if (commandLine.hasOption(Cli.DESIRED_BUFFER_SIZE)) {
          desiredBufferSize = Integer.parseInt(
              commandLine.getOptionValue(Cli.DESIRED_BUFFER_SIZE)) * 1024 * 1024;
        }

        AzureDataLakeStoreUploader adlsUploader = new AzureDataLakeStoreUploader(
            Paths.get(commandLine.getOptionValue(Cli.SOURCE)),
            commandLine.getOptionValue(Cli.CLIENT_ID),
            commandLine.getOptionValue(Cli.AUTH_TOKEN_ENDPOINT),
            commandLine.getOptionValue(Cli.CLIENT_KEY),
            commandLine.getOptionValue(Cli.ACCOUNT_FQDN),
            commandLine.getOptionValue(Cli.DESTINATION),
            desiredParalellism,
            desiredBufferSize);

        if (commandLine.hasOption(Cli.REPROCESS)) {
          if (FolderUtils.cleanUpPartitiallyStagedFiles(
              commandLine.getOptionValue(Cli.SOURCE),
              FolderUtils.COMPLETED_EXTENSION)) {
            logger.info(
                "Reprocessing enabled. Re-staged completed files in folder{}",
                commandLine.getOptionValue(Cli.SOURCE));
          } else {
            logger.info(
                "Reprocessing enabled. Re-staged files in folder{} failed",
                commandLine.getOptionValue(Cli.SOURCE));
          }
        }

        // Clean up partially uploaded files
        if (FolderUtils.cleanUpPartitiallyStagedFiles(
            commandLine.getOptionValue(Cli.SOURCE),
            FolderUtils.INPROGRESS_EXTENSION)) {
          // Now re-fetch the files that qualify to be uploaded
          List<Path> listOfPaths = FolderUtils.getFiles(
              commandLine.getOptionValue(Cli.SOURCE),
              wildCard);
          // Start uploading
          try {
            adlsUploader.upload(listOfPaths);
          } finally {
            // Wait for 5 minutes (not worth a configuration parameter)
            adlsUploader.terminate(300L);
          }
        }

        // Shutdown hook to handle graceful shutdown
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            try {
              logger.info("Shutdown detected. Trying to gracefully shutdown");
              logger.info("Thank you for using the {} ", Cli.HEADER);
              // Wait for 5 minutes (not worth a configuration parameter)
              adlsUploader.terminate(300L);
              mainThread.join();
            } catch (InterruptedException e) {
              logger.error("Cannot gracefully shutdown. "
                  + "Shutdown failed with exception {}", e.getMessage());
              logger.error("Exiting the system with exit code -1");
              System.exit(-1);
            }
          }
        });
      }
    } else {
      logger.error("Error in parsing the command line. Exiting the system with exit code -1");
      System.exit(-1);
    }
  }
}
