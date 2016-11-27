package com.microsoft.azure.adls.db

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Manages the database query execution using the
  * Loan pattern
  *
  * Reference: http://martinsnyder.net/blog/2013/08/07/functional-wrappers-for-legacy-apis/
  */
object DBManager {

  /**
    * Connection String abstraction
    *
    * @param driver              Database driver
    * @param connectionStringUri Connection string Uri
    * @param username            Username
    * @param password            Password
    */
  case class ConnectionInfo(driver: String, connectionStringUri: String, username: String, password: String)

  private val logger = LoggerFactory.getLogger("DBManager")

  /**
    * Applies the supplied function to a managed Scala Iterator wrapping a JDBC result set
    *
    * @param connectionInfo Connection Information
    * @param sqlStatement   SQL Statement to execute
    * @param f              Function applied to result in the resultSet
    * @tparam R Type of the mapped result
    * @return Mapped result
    */
  def withResultSetIterator[T, R](connectionInfo: ConnectionInfo,
                                  sqlStatement: String,
                                  f: (ResultSet) => R,
                                  g: (ResultsIterator[R]) => T): Try[T] = {
    withResultSet[T](connectionInfo,
      sqlStatement, {
        resultSet => {
          val resultsIterator: ResultsIterator[R] = new ResultsIterator[R](resultSet, f)
          g(resultsIterator)
        }
      })
  }

  /**
    * Executes the SQL Statement
    *
    * @param connectionInfo Connection Information
    * @param sqlStatement   SQL Statement to execute
    * @param f              Function to map over resultset
    * @tparam R Type of the mapped result
    * @return Mapped resultset
    */
  def withResultSet[R](connectionInfo: ConnectionInfo,
                       sqlStatement: String,
                       f: (ResultSet) => R): Try[R] = {
    logger.debug(s"Executing SQL Statement: $sqlStatement")

    def g(statement: Statement) = {
      statement.setFetchSize(10000)
      val resultSet = statement executeQuery sqlStatement
      try {
        f(resultSet)
      }
      finally {
        resultSet.close()
      }
    }

    withStatement(connectionInfo, g)
  }

  /**
    * Loan a SQL Statement
    *
    * @param connectionInfo Connection Information
    * @param f              Function that takes a statement and returns the result of type R
    * @tparam R Type of the return value
    * @return Return value
    */
  def withStatement[R](connectionInfo: ConnectionInfo,
                       f: (Statement) => R): Try[R] = {
    def g(connection: Connection) = {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      try {
        f(stmt)
      }
      finally {
        stmt.close()
      }
    }

    withConnection(connectionInfo, g)
  }

  /**
    * Loan a SQL Connection
    *
    * @param connectionInfo Connection Information
    * @param f              Function that takes the connection and returns the result of type R
    * @tparam R Type of the return value
    * @return Return value
    */
  def withConnection[R](connectionInfo: ConnectionInfo,
                        f: (Connection) => R): Try[R] = {
    Class.forName(connectionInfo.driver)
    val dbConnection: Connection = DriverManager getConnection(
      connectionInfo.connectionStringUri,
      connectionInfo.username,
      connectionInfo.password)
    val result = Try(f(dbConnection))
    dbConnection.close()
    result
  }
}

