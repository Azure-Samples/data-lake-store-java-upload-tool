package com.starbucks.analytics.db

/**
 * A trait that defines the methods that support sql data generation
 * for a database backend.
 */
trait SqlGenerator {
  // Generates the sql statement to fetch the metadata
  // for a table including owner, table name, partition & sub-partitions
  def getPartitions(
    owner:         String,
    tables:        List[String],
    partitions:    Option[List[String]],
    subPartitions: Option[List[String]]
  ): String

  // Generates a sql statement to fetch the column names
  // for the specified table
  def getColumnNames(
    owner:     String,
    tableName: String
  ): String

  // Generates a sql statement to fetch data given
  // the metadata information
  def getData(
    schemaInfo: SchemaInfo,
    columns:    List[String]
  ): String
}
