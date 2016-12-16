package com.starbucks.analytics.db
import java.nio.charset.StandardCharsets
import java.sql.ResultSet

import scala.collection.mutable

object Utilities {
  /**
   * Utility function to convert result set to byte array
   *
   * @param resultSet       Result set
   * @param columns         A collection of columns
   * @param columnDelimiter Column separator
   * @param rowDelimiter    Row separator
   * @return Returns a byte array
   */
  def resultSetToByteArray(
    resultSet:       ResultSet,
    columns:         Seq[String],
    columnDelimiter: Char,
    rowDelimiter:    Char
  ): Array[Byte] = {
    val builder: mutable.ArrayBuilder[Byte] = mutable.ArrayBuilder.make[Byte]
    for (columnCount <- 1 to columns.length) {
      if (columnCount > 1) {
        builder += columnDelimiter.toByte
      }
      val datum = resultSet.getString(columnCount)
      if (datum != null)
        builder ++= datum.getBytes(StandardCharsets.UTF_8)
    }
    builder += rowDelimiter.toByte

    builder.result()
  }
}