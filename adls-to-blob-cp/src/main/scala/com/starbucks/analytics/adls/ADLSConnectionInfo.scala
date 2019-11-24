package com.starbucks.analytics.adls

/**
 * Represents the Azure Data Lake Store connection information
 *
 * @param clientId                    Client Id of the application you registered with active directory
 * @param clientKey                   Client key of the application you registered with active directory
 * @param authenticationTokenEndpoint OAuth2 endpoint for the application
 *                                    you registered with active directory
 * @param accountFQDN                 Fully Qualified Domain Name of the Azure data lake store
 */
case class ADLSConnectionInfo(
  clientId:                    String,
  clientKey:                   String,
  authenticationTokenEndpoint: String,
  accountFQDN:                 String
)
