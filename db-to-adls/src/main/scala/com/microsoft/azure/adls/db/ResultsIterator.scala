package com.microsoft.azure.adls.db

import java.sql.ResultSet

/**
  * A private class that implements the Scala iterator interface for JDBC results.
  *
  * Note that the lifetime of the Iterator object must be no longer than the lifetime of
  * this ResultSet object.  This class makes no attempt to manage or close the associated
  * JDBC result set.
  *
  * @param resultSet The JDBC ResultSet object to project as an iterator.
  */
class ResultsIterator[R](resultSet: ResultSet,
                         f: ResultSet => R) extends Iterator[R] {
  /**
    * Retrieves the next row of data from a result set.  Note that this method returns an Option monad
    * If the end of the result set has been reached, it will return None, otherwise it will return Some[Map[String, AnyRef]]
    *
    * @param resultSet JDBC ResultSet to extract row data from
    * @return Some[Map] if there is more row data, or None if the end of the resultSet has been reached
    */
  private def getNextRow(resultSet: ResultSet): Option[R] = {
    if (resultSet.next())
      Some(f(resultSet))
    else
      None
  }

  /**
    * Member variable containing the next row.  We need to manage this state ourselves to defend against implementation
    * changes in how Scala iterators are used.  In particular, we do this to prevent attaching the Scala hasNext function
    * to the ResultSet.next method, which seems generally unsafe.
    */
  private var nextRow = getNextRow(resultSet)

  /**
    * Scala Iterator method called to test if we have more JDBC results
    *
    * @return
    */
  override def hasNext: Boolean = nextRow.isDefined

  /**
    * Scala Iterator method called to retrieve the next JDBC result
    *
    * @return
    */
  override def next(): R = {
    val rowData = nextRow.get
    nextRow = getNextRow(resultSet)
    rowData
  }
}