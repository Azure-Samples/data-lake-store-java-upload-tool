package com.starbucks.analytics.s3

/**
  * Represents the AWS S3 Store connection information
  * @param awsAccessKeyID AWS access key ID for the user.
  * @param awsSecretAccessKey AWS secret access key assigned to the key ID.
  * @param s3BucketName AWS s3 store bucket name.
  * @param s3FolderName AWS s3 store folder inside the given bucket.
  */
case class S3ConnectionInfo(
                           awsAccessKeyID: String,
                           awsSecretAccessKey: String,
                           s3BucketName: String,
                           s3FolderName: String
                           )
