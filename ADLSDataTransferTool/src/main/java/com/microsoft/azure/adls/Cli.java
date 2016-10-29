package com.microsoft.azure.adls;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;

/**
 * Helper class to assist with command line argument parsing.
 */
public class Cli {
  static final String SOURCE = "s";
  static final String CLIENT_ID = "c";
  static final String AUTH_TOKEN_ENDPOINT = "t";
  static final String CLIENT_KEY = "k";
  static final String ACCOUNT_FQDN = "f";
  static final String DESTINATION = "d";
  static final String OCTAL_PERMISSIONS = "o";
  static final String WILDCARD = "w";
  static final String REPROCESS = "r";
  static final String DESIRED_PARALLELISM = "p";
  static final String DESIRED_BUFFER_SIZE = "b";
  static final String HEADER = "Azure Data Lake Store Data Transfer Tool";
  private static final String FOOTER = "Please report issues @ https://www.github.com/gandhinath/DataUploadTools";
  private static final String HELP = "h";
  private static final Logger logger = LoggerFactory.getLogger(Cli.class.getName());
  static String APPLICATION;

  static {
    try {
      APPLICATION = new File(
          Cli.class
              .getProtectionDomain()
              .getCodeSource()
              .getLocation()
              .toURI()
              .getPath()).getName();
    } catch (URISyntaxException s) {
      logger.error("Error getting the jar file name: {}", s.getMessage());
      APPLICATION = "ADLSDataTransferTool.jar";
    }
  }

  private String[] args = null;
  private CommandLine commandLine = null;

  /**
   * Default constructor.
   *
   * @param args Command line arguments
   */
  public Cli(String[] args) {
    this.args = args;
  }

  /**
   * Build the help options.
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
   * Build the options.
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

    Option clientIdOption = Option.builder(CLIENT_ID)
        .argName("clientId")
        .hasArg()
        .required(true)
        .longOpt("clientId")
        .desc("Client Id of the Azure active directory application")
        .type(String.class)
        .build();

    Option authTokenEndpointOption = Option.builder(AUTH_TOKEN_ENDPOINT)
        .argName("authTokenEndpoint")
        .hasArg()
        .required(true)
        .longOpt("authTokenEndpoint")
        .desc("Authentication Token Endpoint of the Azure active directory application")
        .type(String.class)
        .build();

    Option clientKeyOption = Option.builder(CLIENT_KEY)
        .argName("clientKey")
        .hasArg()
        .required(true)
        .longOpt("clientKey")
        .desc("Client key for the Azure active directory application")
        .type(String.class)
        .build();

    Option accountFqdnOption = Option.builder(ACCOUNT_FQDN)
        .argName("accountFQDN")
        .hasArg()
        .required(true)
        .longOpt("accountFQDN")
        .desc("Fully Qualified Domain Name of the Azure data lake account")
        .type(String.class)
        .build();

    Option destinationOption = Option.builder(DESTINATION)
        .argName("destination")
        .hasArg()
        .required(true)
        .longOpt("destination")
        .desc("Root of the ADLS folder path into which the files will be uploaded")
        .type(String.class)
        .build();

    Option octalPermissionsOption = Option.builder(OCTAL_PERMISSIONS)
        .argName("octalPermissions")
        .hasArg()
        .required(true)
        .longOpt("octalPermissions")
        .desc("permissions for the file, as octal digits (For Example, 755)")
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

    Option desiredParallelismOption = Option.builder(DESIRED_PARALLELISM)
        .argName("desiredParallelism")
        .hasArg()
        .required(false)
        .longOpt("desiredParallelism")
        .desc("Desired level of parallelism. "
            + "This will impact your available network bandwidth")
        .type(int.class)
        .build();

    Option desiredBufferSizeOption = Option.builder(DESIRED_BUFFER_SIZE)
        .argName("desiredBufferSize")
        .hasArg()
        .required(false)
        .longOpt("desiredBufferSize")
        .desc("Desired buffer size in megabytes. "
            + "This will impact your available network bandwidth")
        .type(int.class)
        .build();

    options
        .addOption(sourceOption)
        .addOption(clientIdOption)
        .addOption(clientKeyOption)
        .addOption(authTokenEndpointOption)
        .addOption(accountFqdnOption)
        .addOption(destinationOption)
        .addOption(octalPermissionsOption)
        .addOption(wildcardOption)
        .addOption(reprocessOption)
        .addOption(desiredParallelismOption)
        .addOption(desiredBufferSizeOption);

    return options;
  }

  /**
   * Parses the command line arguments using the options setup
   * in the constructor.
   *
   * @return True if parsing is successful
   */
  boolean parse() {
    boolean isParsingSuccessful = false;
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine helpCommandLine = parser.parse(getHelpOptions(), args, true);
      if (helpCommandLine != null && helpCommandLine.hasOption(HELP)) {
        getHelp();
        isParsingSuccessful = false;
      } else {
        this.commandLine = parser.parse(getOptions(), args);
        isParsingSuccessful = true;
      }
    } catch (ParseException e) {
      logger.error("ERROR: {} ", e.getClass());
      logger.error(e.getMessage());
      getHelp();
    }
    return isParsingSuccessful;
  }

  /**
   * Prints out the help message.
   */
  private void getHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(APPLICATION, HEADER, getOptions(), FOOTER, true);
  }

  /**
   * Gets the parsed command line arguments.
   *
   * @return CommandLine
   */
  CommandLine getCommandLine() {
    return this.commandLine;
  }
}
