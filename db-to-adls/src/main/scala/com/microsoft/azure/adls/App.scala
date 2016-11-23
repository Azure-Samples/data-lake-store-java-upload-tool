package com.microsoft.azure.adls

import java.nio.charset.StandardCharsets
import java.sql.ResultSet

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.util.ContextInitializer
import ch.qos.logback.core.joran.spi.JoranException
import com.microsoft.azure.adls.db.{DBManager, OracleMetadata, PartitionMetadata}
import org.slf4j.{Logger, LoggerFactory, Marker, MarkerFactory}

import scala.collection.parallel.ForkJoinTaskSupport

/**
  * Contains utility functions to parse command line arguments.
  */
class App {

  import App._

  /**
    * Parses the command line arguments using scopt
    *
    * @param args Command line arguments
    * @return A valid configuration object parsing was successful
    */
  def parse(args: Array[String]): Option[DataTransferConfig] = {
    val parser = new scopt.OptionParser[DataTransferConfig](getApplicationName) {
      override def showUsageOnError = true

      // Setup the parser
      head(getApplicationName)
      help("help") text "prints this usage text"
      opt[String]("clientId")
        .required()
        .action { (x, c) => c.copy(clientId = x) }
        .text("Client Id of the Azure active directory application")
      opt[String]("authTokenEndpoint")
        .required()
        .action { (x, c) => c.copy(authTokenEndpoint = x) }
        .text("Authentication Token Endpoint of the Azure active directory application")
      opt[String]("clientKey")
        .required()
        .action { (x, c) => c.copy(clientKey = x) }
        .text("Client key for the Azure active directory application")
      opt[String]("accountFQDN")
        .required()
        .action { (x, c) => c.copy(accountFQDN = x) }
        .text("Fully Qualified Domain Name of the Azure data lake account")
      opt[String]("destination")
        .required()
        .action { (x, c) => c.copy(destination = x) }
        .text("Root of the ADLS folder path into which the files will be uploaded")
      opt[String]("octalPermissions")
        .required()
        .action { (x, c) => c.copy(octalPermissions = x) }
        .text("Permissions for the file, as octal digits (For Example, 755)")
      opt[Int]("desiredParallelism")
        .required()
        .action { (x, c) =>
          if (x > 0)
            c.copy(desiredParallelism = x)
          else
            c.copy(desiredParallelism = Runtime.getRuntime.availableProcessors())
        }
        .text("Desired level of parallelism.This will impact your available network bandwidth and source system resources")
      opt[Int]("desiredBufferSize")
        .required()
        .action { (x, c) =>
          if (x > 0)
            c.copy(desiredBufferSize = x)
          else
            c.copy(desiredBufferSize = 256 * 1024 * 1024) // 256 MB by default
        }
        .text("Desired buffer size in megabytes.ADLS,by default, streams 4MB at a time. This will impact your available network bandwidth.")
      opt[String]("logFilePath")
        .required()
        .action { (x, c) => c.copy(logFilePath = x) }
        .text("Log file path")
      opt[Unit]("reprocess")
        .optional()
        .action((_, c) => c.copy(reprocess = true))
        .text("Indicates that you want to reprocess the table and/or partition")
      opt[String]("driver")
        .required()
        .action({ (x, c) => c.copy(driver = x) })
        .text("Name of the jdbc driver")
      opt[String]("connectionStringUrl")
        .required()
        .action({ (x, c) => c.copy(connectStringUrl = x) })
        .text("Connection String Url for the database backend")
      opt[String]("username")
        .required()
        .action({ (x, c) => c.copy(username = x) })
        .text("Username used to connect to the database backend")
      opt[String]("password")
        .required()
        .action({ (x, c) => c.copy(password = x) })
        .text("Password used to connect to the database backend")
      opt[Seq[String]]("source")
        .required()
        .valueName("table1, table2...")
        .action((x, c) => c.copy(tables = x))
        .text("Please provide table names that need to be transferred.")
      opt[Seq[String]]("partitions")
        .optional()
        .valueName("partition1,partition2...")
        .action((x, c) => c.copy(partitions = x))
        .text("Specific partitions that need to be transferred. Can be used for incremental transfer or in combination with reprocess flag")
    }

    // Evaluate
    parser.parse(args, DataTransferConfig()) match {
      case Some(config) =>
        Some(config)
      case None =>
        None
    }
  }

  /**
    * Re-initializes the logger
    *
    * @param logPath Log path
    * @param logFile Log file
    */
  private def reInitializeLogger(logPath: String,
                                 logFile: String): Unit = {
    // Reset the logger context
    System.setProperty("log_path", logPath)
    System.setProperty("log_file", logFile)

    // Reload the logger context
    // to pick up the log path and log file
    val context: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val contextInitializer: ContextInitializer = new ContextInitializer(context)
    context.reset()
    try {
      contextInitializer.autoConfig()
    } catch {
      case e: JoranException =>
        println(s"Error parsing the command line arguments ${e.getMessage}")
    }
  }

  /**
    * Logs a startup message to the log
    *
    * @param logger          Logger used by the application
    * @param applicationName Name of the application
    * @param config          Data Transfer configuration
    */
  private def logStartupMessage(logger: Logger,
                                applicationName: String,
                                config: DataTransferConfig): Unit = {
    logger.info(s"$applicationName starting with command line arguments: ")
    logger.info(s"\t Client id: ${config.clientId}")
    logger.info(s"\t Client key: ${config.clientKey}")
    logger.info(s"\t Authentication token endpoint: ${config.authTokenEndpoint}")
    logger.info(s"\t Account FQDN: ${config.accountFQDN}")
    logger.info(s"\t Destination root folder: ${config.destination}")
    logger.info(s"\t Octal permissions: ${config.octalPermissions}")
    logger.info(s"\t Desired parallelism: ${config.desiredParallelism}")
    logger.info(s"\t Desired buffer size: ${config.desiredBufferSize}MB")
    logger.info(s"\t Log file path: ${config.logFilePath}")
    logger.info(s"\t Re-process triggered: ${config.reprocess}")
    logger.info(s"\t JDBC Driver: ${config.driver}")
    logger.info(s"\t JDBC Connection String Url for the backend: ${config.connectStringUrl}")
    logger.info(s"\t Username used to connect to the database backend: ${config.username}")
    logger.info(s"\t Tables:")
    config.tables.foreach((table) => {
      logger.info(s"\t\t\t $table")
    })
    logger.info(s"\t Partitions:")
    config.partitions.foreach((partition) => {
      logger.info(s"\t\t\t $partition")
    })
  }
}

/**
  * Companion
  */
object App {

  case class DataTransferConfig(
                                 clientId: String = null,
                                 authTokenEndpoint: String = null,
                                 clientKey: String = null,
                                 accountFQDN: String = null,
                                 destination: String = null,
                                 octalPermissions: String = null,
                                 desiredParallelism: Int = 0,
                                 desiredBufferSize: Int = 0,
                                 logFilePath: String = null,
                                 reprocess: Boolean = false,
                                 driver: String = null,
                                 connectStringUrl: String = null,
                                 username: String = null,
                                 password: String = null,
                                 tables: Seq[String] = Seq(),
                                 partitions: Seq[String] = Seq())

  private def getApplicationName: String = new java.io.File(classOf[App]
    .getProtectionDomain
    .getCodeSource
    .getLocation
    .getPath)
    .getName


  /**
    * Entry point for the application.
    *
    * @param args Command line arguments
    */
  def main(args: Array[String]): Unit = {
    val app = new App() with OracleMetadata

    val config = app.parse(args)
    if (config.isEmpty) {
      System.exit(-1)
    }

    app.reInitializeLogger(config.get.logFilePath, getApplicationName)

    val logger = LoggerFactory.getLogger(classOf[App])
    app.logStartupMessage(logger, getApplicationName, config.get)

    // Collect the metadata required for
    // fetching the data from the source database
    val metadataCollection: Seq[PartitionMetadata] = DBManager.sql[PartitionMetadata](config.get.driver,
      config.get.connectStringUrl,
      config.get.username,
      config.get.password,
      app.generateSqlToGetPartitions(
        config.get.tables,
        config.get.partitions), { resultSet: ResultSet =>
        PartitionMetadata(resultSet.getString(1),
          Option(resultSet.getString(2)),
          Option(resultSet.getString(3)))
      })
    metadataCollection.foreach(metadata =>
      logger.info(s"Table: ${metadata.tableName}, Partition: ${metadata.partitionName}, Sub-Partition: ${metadata.subPartitionName}"))

    // Setup support for parallelism
    val parMetadataCollection = metadataCollection.par
    parMetadataCollection.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(config.get.desiredParallelism))
    // Iterate through the metadata collection
    // and go through the algorithm:
    parMetadataCollection.foreach(metadata => {
      val path: String = ADLSUploader.getADLSPath(config.get.destination, metadata)
      val parentMarker: Marker = MarkerFactory.getMarker("DATA TRANSFER")
      val childMarker: Marker = MarkerFactory.getMarker(path)
      parentMarker.add(childMarker)

      logger.info(childMarker,
        s"""Initializing transfer of Table: ${metadata.tableName}, Partition: ${metadata.partitionName},
           |Sub-Partition: ${metadata.subPartitionName}, Destination: $path""".stripMargin)

      // 1. Initialize the uploader
      val uploader = ADLSUploader(config.get.clientId,
        config.get.clientKey,
        config.get.authTokenEndpoint,
        config.get.accountFQDN,
        path,
        config.get.octalPermissions,
        config.get.desiredBufferSize * 1024 * 1024)

      // 2. Get the column list
      // 3. Upload the header string
      val columnCollection: Seq[String] = DBManager.sql[String](config.get.driver,
        config.get.connectStringUrl,
        config.get.username,
        config.get.password,
        app.generateSqlToGetColumnNames(metadata.tableName), { resultSet: ResultSet =>
          resultSet.getString(1)
        })
      uploader.bufferedUpload(s"${columnCollection.mkString("\\t")}\\n"
        .getBytes(StandardCharsets.UTF_8))

      // 4. Fetch the data
      // 5. Convert data to byte array
      // 6. Upload the data to Azure Data Lake Store
      DBManager.sql[Array[Byte]](config.get.driver,
        config.get.connectStringUrl,
        config.get.username,
        config.get.password,
        app.generateSqlToGetDataByPartition(metadata, columnCollection), { resultSet: ResultSet =>
          DBManager.resultSetToByteArray(resultSet,
            columnCollection,
            "\\t",
            "\\n")
        }).foreach(uploader.bufferedUpload)

      uploader.close()

      logger.info(childMarker,
        s"""Completed transfer of Table: ${metadata.tableName}, Partition: ${metadata.partitionName},
           |Sub-Partition: ${metadata.subPartitionName}, Destination: $path""".stripMargin)
    })
  }
}