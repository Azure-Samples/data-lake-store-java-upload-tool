package com.microsoft.azure.adls;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) throws ParseException, InterruptedException {
        // Parse the command line arguments
        // prepare for execution
        Cli cli = new Cli(args);
        if (cli.parse()) {
            CommandLine commandLine = cli.getCommandLine();
            Option[] options = commandLine.getOptions();
            if (options.length > 0) {
                logger.info("Starting {} with the following command line arguments", Cli.HEADER);
                for (Option option : options) {
                    logger.info("{} ({}): {}",
                            option.getLongOpt(),
                            option.getDescription(),
                            option.hasArg() ? commandLine.getOptionValue(option.getOpt()) : "TRUE");
                }
            }
        } else {
            logger.error("Error in parsing the command line. Exiting the system with exit code -1");
            System.exit(-1);
        }

        // Shutdown hook to handle graceful shutdown
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    logger.info("Shutdown detected. Trying to gracefully shutdown");
                    mainThread.join();
                } catch (InterruptedException e) {
                    logger.error("Cannot gracefully shutdown. Shutdown failed with exception {}", e.getMessage());
                    logger.error("Exiting the system with exit code -1");
                    System.exit(-1);
                }
            }
        });
    }
}
