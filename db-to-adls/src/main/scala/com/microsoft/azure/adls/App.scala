package com.microsoft.azure.adls

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.util.ContextInitializer
import ch.qos.logback.core.joran.spi.JoranException
import org.slf4j.LoggerFactory

/**
  * Contains utility functions to parse command line arguments.
  */
class App {

  case class Config(
                     clientId: String = null,
                     authTokenEndpoint: String = null,
                     clientKey: String = null,
                     accountFQDN: String = null,
                     destination: String = null,
                     octalPermissions: String = null,
                     desiredParallelism: Int = 0,
                     desiredBufferSize: Int = 0,
                     logFilePath: String = null)

  def getApplicationName: String = new java.io.File(classOf[App]
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
      opt[String]('c', "clientId")
        .required()
        .action { (x, c) => c.copy(clientId = x) }
        .text("Client Id of the Azure active directory application")
      opt[String]('t', "authTokenEndpoint")
        .required()
        .action { (x, c) => c.copy(authTokenEndpoint = x) }
        .text("Authentication Token Endpoint of the Azure active directory application")
      opt[String]('k', "clientKey")
        .required()
        .action { (x, c) => c.copy(clientKey = x) }
        .text("Client key for the Azure active directory application")
      opt[String]('a', "accountFQDN")
        .required()
        .action { (x, c) => c.copy(accountFQDN = x) }
        .text("Fully Qualified Domain Name of the Azure data lake account")
      opt[String]('d', "destination")
        .required()
        .action { (x, c) => c.copy(destination = x) }
        .text("Root of the ADLS folder path into which the files will be uploaded")
      opt[String]('o', "octalPermissions")
        .required()
        .action { (x, c) => c.copy(octalPermissions = x) }
        .text("Permissions for the file, as octal digits (For Example, 755)")
      opt[Int]('p', "desiredParallelism")
        .required()
        .action { (x, c) =>
          if (x > 0)
            c.copy(desiredParallelism = x)
          else
            c.copy(desiredParallelism = Runtime.getRuntime.availableProcessors())
        }
        .text("Desired level of parallelism.This will impact your available network bandwidth")
      opt[Int]('b', "desiredBufferSize")
        .required()
        .action { (x, c) =>
          if (x > 0)
            c.copy(desiredBufferSize = x)
          else
            c.copy(desiredBufferSize = 256 * 1024 * 1024) // 256 MB by default
        }
        .text("Desired buffer size in megabytes.This will impact your available network bandwidth")
      opt[String]('l', "logFilePath")
        .required()
        .action { (x, c) => c.copy(logFilePath = x) }
        .text("Log file path")
    }

    // Evaluate
    parser.parse(args, Config()) match {
      case Some(config) =>
        Some(config)
      case None =>
        None
    }
  }
}

/**
  * Companion
  */
object App {
  val logger = LoggerFactory.getLogger(classOf[App])

  /**
    * Entry point for the application.
    *
    * @param args Command line arguments
    */
  def main(args: Array[String]): Unit = {
    val app = new App()

    val config = app.parse(args)
    if (config.isEmpty) {
      System.exit(-1)
    }

    logger.info(s"${app.getApplicationName} starting with command line arguments: ")
    logger.info(s"\t Client id: ${config.get.clientId}")
    logger.info(s"\t Client key: ${config.get.clientKey}")
    logger.info(s"\t Authentication token endpoint: ${config.get.authTokenEndpoint}")
    logger.info(s"\t Account FQDN: ${config.get.accountFQDN}")
    logger.info(s"\t Destination root folder: ${config.get.destination}")
    logger.info(s"\t Octal permissions: ${config.get.octalPermissions}")
    logger.info(s"\t Desired parallelism: ${config.get.desiredParallelism}")
    logger.info(s"\t Desired buffer size: ${config.get.desiredBufferSize}")
    logger.info(s"\t Log file path: ${config.get.logFilePath}")

    // Reset the logger context
    System.setProperty("log_path", config.get.logFilePath)
    System.setProperty("log_file", s"${app.getApplicationName}.log")
    // Reload the logger context
    // to pick up the log path and log file
    val context: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val contextInitializer: ContextInitializer = new ContextInitializer(context)
    context.reset()
    try {
      contextInitializer.autoConfig()
    } catch {
      case e: JoranException => logger.error("Error parsing the command line arguments", e)
    }
  }
}