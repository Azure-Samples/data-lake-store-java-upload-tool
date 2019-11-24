package com.starbucks.analytics.blob

/**
 * Represents the Azure Data Lake Store connection information
 *
 * @param accountName                  Name of the blob storage account
 * @param accountKey                   Key to access the blob storage account
 */
case class BlobConnectionInfo(
  accountName: String,
  accountKey:  String
)
