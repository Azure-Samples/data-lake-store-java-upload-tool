package com.starbucks.analytics

/**
 * Represents the options used for copying from the database to
 * Azure Data Lake
 * @param desiredBufferSize Desired buffer size in bytes
 * @param desiredParallelism Desired level of parallelism
 * @param fetchSize Number of rows to be fetched from the database
 * @param separator Column Separator
 */
case class UploaderOptionsInfo(
  desiredBufferSize:  Int,
  desiredParallelism: Int,
  fetchSize:          Int,
  separator:          Char
)
