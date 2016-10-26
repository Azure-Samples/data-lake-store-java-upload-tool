package com.microsoft.azure.adls;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;

/**
 * Helper class to assist with command line argument parsing
 */
public class Cli {
    private static final Logger logger = LoggerFactory.getLogger(Cli.class.getName());

    public static String APPLICATION;

    static {
        try {
            APPLICATION = new File(Cli.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getName();
        } catch (URISyntaxException s) {
            logger.error("Error getting the jar file name: {}", s.getMessage());
            APPLICATION = "ADLSDataTransferTool.jar";
        }
    }

    public static final String HEADER = "Azure Data Lake Store Data Transfer Tool";
    public static final String FOOTER = "Please report issues @ https://www.github.com/gandhinath/DataUploadTools";

    public static final String SOURCE = "s";
    public static final String WILDCARD = "w";
    public static final String REPROCESS = "r";
    public static final String NUMBER_OF_THREADS = "n";
    public static final String HELP = "h";

    private String[] args = null;
    private CommandLine commandLine = null;

    /**
     * Default constructor
     *
     * @param args Command line arguments
     */
    public Cli(String[] args) {
        this.args = args;
    }

    /**
     * Build the help options
     *
     * @return Options
     */
    private static Options getHelpOptions() {
        Options options = new Options();

        Option helpOption = Option.builder(HELP)
                .argName("help")
                .required(false)
                .longOpt("help")
                .desc("Prints the help text")
                .type(Boolean.class)
                .build();

        options.addOption(helpOption);

        return options;
    }

    /**
     * Build the options
     *
     * @return Options
     */
    private static Options getOptions() {
        Options options = new Options();

        Option reprocessOption = Option.builder(REPROCESS)
                .argName("reprocess")
                .required(false)
                .longOpt("reprocess")
                .desc("Indicates that you want to reprocess the files")
                .type(Boolean.class)
                .build();

        Option sourceOption = Option.builder(SOURCE)
                .argName("source")
                .hasArg()
                .required(true)
                .longOpt("source")
                .desc("Root of the folder path that contains the files to upload")
                .type(String.class)
                .build();

        Option wildcardOption = Option.builder(WILDCARD)
                .argName("wildcard")
                .hasArg()
                .required(false)
                .longOpt("wildcard")
                .desc("Regular expression to upload the files that match a specific pattern")
                .type(String.class)
                .build();

        Option numberOfThreadsOption = Option.builder(NUMBER_OF_THREADS)
                .argName("numberOfThreads")
                .hasArg()
                .required(true)
                .longOpt("numberOfThreads")
                .desc("Number of parallel upload threads. This will impact your available network bandwidth")
                .type(Short.class)
                .build();

        options.addOption(sourceOption);
        options.addOption(wildcardOption);
        options.addOption(reprocessOption);
        options.addOption(numberOfThreadsOption);

        return options;
    }

    /**
     * Parses the command line arguments using the options setup
     * in the constructor
     *
     * @return True if parsing is successful
     */
    boolean parse() {
        boolean isParsingSuccessful = false;
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine c = parser.parse(getHelpOptions(), args, true);
            if (c != null && c.hasOption(HELP)) {
                getHelp();
                isParsingSuccessful = true;
            } else {
                this.commandLine = parser.parse(getOptions(), args);
                isParsingSuccessful = true;
            }
        } catch (ParseException e) {
            logger.error("ERROR: " + e.getClass());
            logger.error(e.getMessage());
            getHelp();
        }
        return isParsingSuccessful;
    }

    /**
     * Prints out the help message
     */
    private void getHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(APPLICATION, HEADER, getOptions(), FOOTER, true);
    }

    /**
     * Gets the parsed command line arguments
     *
     * @return CommandLine
     */
    CommandLine getCommandLine() {
        return this.commandLine;
    }
}
