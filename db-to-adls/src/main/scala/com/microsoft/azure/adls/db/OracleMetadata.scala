package com.microsoft.azure.adls.db

/**
  * Implementation of the Metadata trait that is specific to Oracle
  */
trait OracleMetadata extends Metadata {
  /**
    * SQL Statement to generate the list of available partitions
    *
    * @param tables     List of tables
    * @param partitions List of partitions
    * @return SQL Statement
    */
  override def generateSqlToGetPartitions(tables: Seq[String],
                                          partitions: Seq[String]): String = {
    val builder: StringBuilder = new StringBuilder
    builder ++=
      s"""SELECT T.TABLE_NAME, P.PARTITION_NAME, SP.SUBPARTITION_NAME FROM
         | ALL_TABLES T
         | LEFT OUTER JOIN ALL_TAB_PARTITIONS P ON
         | T.TABLE_NAME = P.TABLE_NAME
         | LEFT OUTER JOIN ALL_TAB_SUBPARTITIONS SP ON
         | P.TABLE_NAME = SP.TABLE_NAME and P.PARTITION_NAME = SP.PARTITION_NAME
         | WHERE T.TABLE_NAME IN (${tables map (table => s"'$table'") mkString ", "})
       """.stripMargin
    if (partitions.nonEmpty) {
      builder ++= s" AND P.PARTITION_NAME IN (${partitions map (partition => s"'$partition'") mkString ", "})"
    }

    builder.toString()
  }

  /**
    * SQL Statement to generate a list of columns
    *
    * @param tableName List of tables
    * @return SQL Statement
    */
  override def generateSqlToGetColumnNames(tableName: String): String = {
    s"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '$tableName' ORDER BY COLUMN_NAME"
  }

  /**
    * SQL Statement to fetch the data from a given partition
    *
    * @param partitionMetadata Partition metadata that contains table name, partition name and
    *                          sub partition name
    * @param columns           List of columns to fetch
    * @return SQL Statement to fetch the data
    */
  override def generateSqlToGetDataByPartition(partitionMetadata: PartitionMetadata, columns: Seq[String]): String = {
    partitionMetadata.subPartitionName match {
      case Some(subPartition) =>
        s"SELECT ${columns mkString ","} FROM ${partitionMetadata.tableName} SUBPARTITION($subPartition)"
      case None =>
        partitionMetadata.partitionName match {
          case Some(partition) =>
            s"SELECT ${columns mkString ","} FROM ${partitionMetadata.tableName} SUBPARTITION($partition)"
          case None =>
            s"SELECT ${columns mkString ","} FROM ${partitionMetadata.tableName}"
        }
    }
  }
}