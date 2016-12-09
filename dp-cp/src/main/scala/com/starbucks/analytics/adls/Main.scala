package com.starbucks.analytics.adls

import java.io.File

import org.slf4j.{ Logger, LoggerFactory }

/**
 * Entry point for the application
 * Orchestrator
 */
object Main extends App {
  val rootLogger: Logger = LoggerFactory.getLogger(
    org.slf4j.Logger.ROOT_LOGGER_NAME
  )

  // Parse the command line arguments
  // Exit if there is a problem parsing the command line arguments
  val config = parse(args)
  if (config.isEmpty) {
    System.exit(-1)
  }

  logStartupMessage(rootLogger, getApplicationName, config.get)

  // Configuration object
  case class Config(file: File = new File("."))

  // Utility function to return the application name
  private def getApplicationName: String = new java.io.File(classOf[App]
    .getProtectionDomain
    .getCodeSource
    .getLocation
    .getPath)
    .getName

  /**
   * Parses the command line arguments using scopt
   *
   * @param args Command line arguments
   * @return A valid configuration object parsing was successful
   */
  def parse(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config](getApplicationName) {
      override def showUsageOnError = true

      // Setup the parser
      head(getApplicationName)
      help("help") text "prints this usage text"
      opt[File]('f', "fileName")
        .valueName("<file name>")
        .required()
        .validate(f =>
          if (f exists) success
          else failure(s"The file ${f.getAbsolutePath} should exist."))
        .action { (x, c) => c.copy(file = x) }
        .text("File containing the uploader")
    }

    // Evaluate
    parser.parse(args, Config()) match {
      case Some(config) =>
        Some(config)
      case None =>
        None
    }
  }

  /**
   * Logs a startup message to the log
   *
   * @param logger          Logger used by the application
   * @param applicationName Name of the application
   * @param config          Data Transfer configuration
   */
  private def logStartupMessage(
    logger:          Logger,
    applicationName: String,
    config:          Config
  ): Unit = {
    logger.info(s"$applicationName starting with command line arguments: ")
    logger.info(s"\t Filename: ${config.file.getAbsolutePath}")
  }
}
