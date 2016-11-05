package com.microsoft.azure.adls;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        // Prepare for execution
        String wildCard = "**/*";
        int desiredParallelism = Runtime.getRuntime().availableProcessors();
        int desiredBufferSize = 256 * 1024 * 1024;  // 256 MB by default
        String logFilePath = System.getProperty("user.dir");

        if (commandLine.hasOption(Cli.WILDCARD)) {
          wildCard = commandLine.getOptionValue(Cli.WILDCARD);
        }
        if (commandLine.hasOption(Cli.DESIRED_PARALLELISM)) {
          desiredParallelism = Integer.parseInt(
              commandLine.getOptionValue(Cli.DESIRED_PARALLELISM));
        }
        if (commandLine.hasOption(Cli.DESIRED_BUFFER_SIZE)) {
          desiredBufferSize = Integer.parseInt(
              commandLine.getOptionValue(Cli.DESIRED_BUFFER_SIZE)) * 1024 * 1024;
        }
        if (commandLine.hasOption(Cli.LOG_FILE_PATH)) {
          logFilePath = commandLine.getOptionValue(Cli.LOG_FILE_PATH);
        }

        // Reset the logger context
        System.setProperty("log_path", logFilePath);
        System.setProperty("log_file", Cli.APPLICATION);

        // Reload the logger context
        // to pick up the log path and log file
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ContextInitializer contextInitializer = new ContextInitializer(loggerContext);
        loggerContext.reset();
        try {
          contextInitializer.autoConfig();
        } catch (JoranException e) {
          e.printStackTrace();
        }

        logger.info("Starting {} with the following command line arguments", Cli.APPLICATION);
        for (Option option : options) {
          logger.info("{} ({}): {}",
              option.getLongOpt(),
              option.getDescription(),
              option.hasArg() ? commandLine.getOptionValue(option.getOpt()) : "TRUE");
        }

        // If re-processing is detected, re-set the status of the completed files
        // so it can be picked up by the uploader.
        if (commandLine.hasOption(Cli.REPROCESS)) {
          if (FolderUtils.cleanUpPartitiallyStagedFiles(
              commandLine.getOptionValue(Cli.SOURCE),
              FolderUtils.COMPLETED_EXTENSION)) {
            logger.info(
                "Reprocessing enabled. Re-staged completed files in folder{}",
                commandLine.getOptionValue(Cli.SOURCE));
          } else {
            logger.info(
                "Reprocessing enabled. Re-staging files in folder{} failed",
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
          try (AzureDataLakeStoreUploader adlsUploader = new AzureDataLakeStoreUploader(
              Paths.get(commandLine.getOptionValue(Cli.SOURCE)),
              commandLine.getOptionValue(Cli.CLIENT_ID),
              commandLine.getOptionValue(Cli.AUTH_TOKEN_ENDPOINT),
              commandLine.getOptionValue(Cli.CLIENT_KEY),
              commandLine.getOptionValue(Cli.ACCOUNT_FQDN),
              commandLine.getOptionValue(Cli.DESTINATION),
              commandLine.getOptionValue(Cli.OCTAL_PERMISSIONS),
              desiredParallelism,
              desiredBufferSize)) {
            adlsUploader.upload(listOfPaths);
          } catch (IOException ex) {
            logger.error("Instantiating AzureDataLakeStoreUploader and upload the list of files "
                + "failed with exception {}", ex.getMessage());
          } catch (Exception ex) {
            logger.error("Instantiating AzureDataLakeStoreUploader and upload the list of files "
                + "failed with an unknown exception {}", ex.getMessage());
          }
        }
      }
    } else {
      logger.error("Error in parsing the command line. Exiting the system with exit code -1");
      System.exit(-1);
    }
  }
}
