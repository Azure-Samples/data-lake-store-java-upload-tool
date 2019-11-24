package com.starbucks.analytics.keyvault

/**
 * Represents the Azure Key Vault connection information
 *
 * @param clientId                    Client Id of the application you registered with active directory
 * @param clientKey                   Client key of the application you registered with active directory
 */
case class KeyVaultConnectionInfo(
  clientId:  String,
  clientKey: String
)
