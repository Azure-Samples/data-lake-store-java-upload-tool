package com.microsoft.azure.adls.db

import java.nio.charset.StandardCharsets
import java.sql.ResultSet

import scala.collection.mutable

object Utilities {
  /**
    * Utility function to convert result set to byte array
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
    var builder: mutable.ArrayBuilder[Byte] = mutable.ArrayBuilder.make[Byte]
    for (columnCount <- 1 to columns.length) {
      if (columnCount > 1) {
        builder ++= columnSeparator.getBytes(StandardCharsets.UTF_8)
      }
      val datum = resultSet.getString(columnCount)
      if (datum != null)
        builder ++= datum.getBytes(StandardCharsets.UTF_8)
    }
    builder ++= rowSeparator.getBytes(StandardCharsets.UTF_8)

    builder.result()
  }
}
