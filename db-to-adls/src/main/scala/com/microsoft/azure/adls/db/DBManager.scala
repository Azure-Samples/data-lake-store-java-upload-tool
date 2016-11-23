package com.microsoft.azure.adls.db

import java.sql.{Connection, DriverManager, ResultSet, Statement}


/**
  * Manages the database query execution using the
  * Loan pattern
  */
object DBManager {
  /**
    * Stream a SQL Query
    *
    * @param driver              Database driver
    * @param connectionStringUri Connection string Uri
    * @param username            Username
    * @param password            Password
    * @param sqlStatement        SQL Statement to execute
    * @return Resultset Stream
    */
  def sql(driver: String,
          connectionStringUri: String,
          username: String,
          password: String,
          sqlStatement: String): Stream[ResultSet] = {
    withStatement(driver,
      connectionStringUri,
      username,
      password, {
        statement =>
          val resultSet = statement executeQuery sqlStatement
          new Iterator[ResultSet] {
            def hasNext = resultSet.next

            def next = resultSet
          }.toStream
      })
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

