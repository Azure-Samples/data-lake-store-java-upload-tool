package com.starbucks.analytics.db

/**
 * Container of the metadata information
 *
 * @param owner            Owner of the table
 * @param tableName        Name of table
 * @param partitionName    Name of the partition
 * @param subPartitionName Name of the sub-partition
 */
case class SchemaInfo(
  owner:            String,
  tableName:        String,
  partitionName:    Option[String],
  subPartitionName: Option[String]
)
