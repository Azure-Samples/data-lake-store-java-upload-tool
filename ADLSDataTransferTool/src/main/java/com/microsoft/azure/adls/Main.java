package com.microsoft.azure.adls;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (commandLine.hasOption(Cli.WILDCARD)) {
          wildCard = commandLine.getOptionValue(Cli.WILDCARD);
        }
        if (commandLine.hasOption(Cli.DESIRED_PARALLELISM)) {
          desiredParalellism = Integer.parseInt(
              commandLine.getOptionValue(Cli.DESIRED_PARALLELISM));
        }

        // Start the execution
        AzureDataLakeStoreUploader adlsUploader = new AzureDataLakeStoreUploader(
            desiredParalellism);

        FolderUtils.getFiles(
            commandLine.getOptionValue(Cli.SOURCE),
            wildCard);

        // Shutdown hook to handle graceful shutdown
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            try {
              logger.info("Shutdown detected. Trying to gracefully shutdown");
              logger.info("Thanks for using the application {}", Cli.APPLICATION);
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
