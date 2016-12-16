package com.starbucks.analytics.db

/**
 * Database Connection String abstraction
 *
 * @param driver              Database driver
 * @param connectionStringUri Connection string Uri
 * @param username            Username
 * @param password            Password
 */
case class DBConnectionInfo(
  driver:              String,
  connectionStringUri: String,
  username:            String,
  password:            String
)
