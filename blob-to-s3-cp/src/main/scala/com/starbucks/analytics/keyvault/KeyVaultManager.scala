package com.starbucks.analytics.keyvault

import java.util.concurrent.Executors

import com.microsoft.azure.keyvault.core.IKey
import com.typesafe.scalalogging.Logger
import com.microsoft.azure.keyvault.{KeyVaultClient, KeyVaultClientImpl, KeyVaultClientService, KeyVaultConfiguration}
import com.microsoft.azure.keyvault.extensions.KeyVaultKeyResolver
import org.apache.http.impl.client.HttpClientBuilder

import scala.util.Try

object KeyVaultManager {
  private val logger = Logger("KeyVaultManager")

  /**
   * Loan a Azure Key Vault client
   *
   * @param f              Function that takes the Azure Data Lake Key Vault
   *                       client and returns the result of type R
   * @tparam R Type of the return value
   * @return Return value
   */
  private def withAzureKeyVaultClient[R](
    keyVaultConnectionInfo: KeyVaultConnectionInfo,
    f:                      (KeyVaultClient) => R
  ): Try[R] = {
    val httpClientBuilder = HttpClientBuilder.create()
    val executorService = Executors.newCachedThreadPool()
    val credentials = new KVCredentials(keyVaultConnectionInfo)
    val cloudVault: KeyVaultClientImpl = new KeyVaultClientImpl(
      httpClientBuilder,
      executorService,
      credentials
    )
    val result = Try(f(cloudVault))
    result
  }

  def getKey(
    keyVaultConnectionInfo: KeyVaultConnectionInfo,
    resourceUri:                String
  ): Try[IKey] = {
      def fn(kvClient: KeyVaultClient): IKey = {
        val keyVaultResolver: KeyVaultKeyResolver = new KeyVaultKeyResolver(
          kvClient
        )
        keyVaultResolver.resolveKeyAsync(resourceUri).get()
      }
    withAzureKeyVaultClient(
      keyVaultConnectionInfo,
      fn
    )
  }

}
