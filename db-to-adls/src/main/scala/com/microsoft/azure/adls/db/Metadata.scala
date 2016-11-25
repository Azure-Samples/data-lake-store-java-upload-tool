package com.microsoft.azure.adls.db

/**
  * Container of the metadata information
  *
  * @param tableName        Name of table
  * @param partitionName    Name of the partition
  * @param subPartitionName Name of the sub-partition
  */
case class PartitionMetadata(tableName: String,
                             partitionName: Option[String],
                             subPartitionName: Option[String])

/**
  * A trait that defines the methods that support metadata extraction
  * for a database backend.
  */
trait Metadata {

  def generateSqlToGetPartitions(tables: List[String],
                                 partitions: List[String]): String

  def generateSqlToGetColumnNames(tableName: String): String

  def generateSqlToGetDataByPartition(partitionMetadata: PartitionMetadata,
                                      columns: List[String]): String
}
