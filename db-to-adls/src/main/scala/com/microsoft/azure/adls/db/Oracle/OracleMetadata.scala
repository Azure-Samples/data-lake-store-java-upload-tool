package com.microsoft.azure.adls.db.Oracle

import com.microsoft.azure.adls.db.{Metadata, PartitionMetadata}

import scala.collection.mutable

/**
  * Implementation of the Metadata trait that is specific to Oracle
  */
trait OracleMetadata extends Metadata {
  /**
    * SQL Statement to generate the list of available partitions
    *
    * @param tables        List of tables
    * @param partitions    List of partitions
    * @param subPartitions List of sub-partitions
    * @return SQL Statement
    */
  override def generateSqlToGetPartitions(tables: List[String],
                                          partitions: List[String],
                                          subPartitions: List[String]): String = {
    val builder: StringBuilder = new StringBuilder
    builder ++=
      s"""SELECT T.TABLE_NAME, P.PARTITION_NAME, SP.SUBPARTITION_NAME FROM
         | ALL_TABLES T
         | LEFT OUTER JOIN ALL_TAB_PARTITIONS P ON
         | T.TABLE_NAME = P.TABLE_NAME
         | LEFT OUTER JOIN ALL_TAB_SUBPARTITIONS SP ON
         | P.TABLE_NAME = SP.TABLE_NAME and P.PARTITION_NAME = SP.PARTITION_NAME
         | WHERE T.TABLE_NAME IN (${tables map (table => s"'${table.toUpperCase}'") mkString ", "})
       """.stripMargin
    if (partitions.nonEmpty) {
      builder ++= " AND( "
      builder ++= partitions.foldLeft(new StringBuilder) {
        (sb, s) => sb append s"OR P.PARTITION_NAME LIKE '%${s.toUpperCase}%' "
      }.delete(0, 3).toString
      builder ++= ")"
    }
    if (subPartitions.nonEmpty) {
      builder ++= " AND( "
      builder ++= subPartitions.foldLeft(new StringBuilder) {
        (sb, s) => sb append s"OR SP.SUBPARTITION_NAME LIKE '%${s.toUpperCase}%' "
      }.delete(0, 3).toString
      builder ++= ")"
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
    * @param hints             Database query hints primarily geared towards query optimization
    * @return SQL Statement to fetch the data
    */
  override def generateSqlToGetDataByPartition(partitionMetadata: PartitionMetadata,
                                               columns: List[String],
                                               hints: mutable.Map[String, AnyVal]): String = {
    partitionMetadata.subPartitionName match {
      case Some(subPartition) =>
        s"SELECT ${columns mkString ","} FROM ${partitionMetadata.tableName} SUBPARTITION($subPartition)"
      case None =>
        partitionMetadata.partitionName match {
          case Some(partition) =>
            s"SELECT ${columns mkString ","} FROM ${partitionMetadata.tableName} SUBPARTITION($partition)"
          case None =>
            val parallelTag: StringBuilder = new StringBuilder
            if (hints.contains(ParallelismHintTag)) {
              parallelTag.append("/* PARALLEL(")
              parallelTag.append(partitionMetadata.tableName)
              parallelTag.append(s" ${hints get ParallelismHintTag}")
              parallelTag.append(") */")
            }
            s"""SELECT
               | ${parallelTag.toString}
               | ${columns mkString ","} FROM
               | ${partitionMetadata.tableName}
             """.stripMargin
        }
    }
  }
}
