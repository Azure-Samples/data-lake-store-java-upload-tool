package com.microsoft.azure.adls.db

import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Manages the database query execution using the
  * Loan pattern
  */
object DBManager {
  private val logger = LoggerFactory.getLogger("DBManager")

  /**
    * Stream a SQL Query
    *
    * @param driver              Database driver
    * @param connectionStringUri Connection string Uri
    * @param username            Username
    * @param password            Password
    * @param sqlStatement        SQL Statement to execute
    * @param f                   Function to map over resultset
    * @tparam R Type of the mapped result
    * @return Mapped resultset
    */
  def sql[R](driver: String,
             connectionStringUri: String,
             username: String,
             password: String,
             sqlStatement: String,
             f: (ResultSet) => R): Stream[R] = {
    logger.debug(s"Executing SQL Statement: $sqlStatement")
    withStatement(driver,
      connectionStringUri,
      username,
      password, {
        statement =>
          val resultSet = statement executeQuery sqlStatement
          new Iterator[R] {
            def hasNext: Boolean = resultSet.next

            def next: R = f(resultSet)
          }
      }).toStream
  }

  /**
    * Convert result set to byte array
    *
    * @param resultSet       Result set
    * @param columns         A collection of columns
    * @param columnSeparator Column separator
    * @param rowSeparator    Row separator
    * @return Returns a byte array
    */
  def resultSetToByteArray(resultSet: ResultSet,
                           columns: Seq[String],
                           columnSeparator: String,
                           rowSeparator: String): Array[Byte] = {
    var builder: mutable.ArrayBuilder[Byte] = new mutable.ArrayBuilder.ofByte
    for (columnCount <- 1 to columns.length) {
      if (columnCount > 1) {
        builder ++= columnSeparator.getBytes(StandardCharsets.UTF_8)
      }
      val datum = resultSet.getBytes(columnCount)
      if (datum != null)
        builder ++= datum
    }
    builder ++= rowSeparator.getBytes(StandardCharsets.UTF_8)

    builder.result()
  }

  /**
    * Loan a SQL Statement
    *
    * @param driver              Database driver
    * @param connectionStringUri Connection string Uri
    * @param username            Username
    * @param password            Password
    * @param f                   Function that takes a statement and returns the result of type R
    * @tparam R Type of the return value
    * @return Return value
    */
  private def withStatement[R](driver: String,
                               connectionStringUri: String,
                               username: String,
                               password: String,
                               f: (Statement) => R): R = {
    withConnection(
      driver,
      connectionStringUri,
      username,
      password, {
        connection => f(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
      })
  }

  /**
    * Loan a SQL Connection
    *
    * @param driver              Database driver
    * @param connectionStringUri Connection string Uri
    * @param username            Username
    * @param password            Password
    * @param f                   Function that takes the connection and returns the result of type R
    * @tparam R Type of the return value
    * @return Return value
    */
  private def withConnection[R](driver: String,
                                connectionStringUri: String,
                                username: String,
                                password: String,
                                f: (Connection) => R): R = {
    Class.forName(driver)
    val dbConnection: Connection = DriverManager getConnection(connectionStringUri, username, password)
    try
      f(dbConnection)
    finally
      dbConnection.close()
  }
}

