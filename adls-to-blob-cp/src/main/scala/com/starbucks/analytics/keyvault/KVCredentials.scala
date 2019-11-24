package com.starbucks.analytics.keyvault

import java.util
import java.util.concurrent.{ ExecutorService, Executors, Future }

import com.microsoft.aad.adal4j.{ AuthenticationContext, AuthenticationResult, ClientCredential }
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials
import com.microsoft.windowsazure.core.pipeline.filter.ServiceRequestContext
import org.apache.http.Header
import org.apache.http.message.BasicHeader

/**
 * A class that stores KeyVault credentials and knows how to respond to
 * authentication challenges. See KeyVaultCredentials for more information.
 *
 * @param keyVaultConnectionInfo Key Vault Connection Information
 */
class KVCredentials(keyVaultConnectionInfo: KeyVaultConnectionInfo) extends KeyVaultCredentials {
  private val connectionInfo: KeyVaultConnectionInfo = keyVaultConnectionInfo

  /**
   * Actually do the authentication. This method will be called by the super
   * class.
   *
   * @param request The request being sent
   * @param challenge Information about the challenge from the service.
   */
  override def doAuthenticate(
    request:   ServiceRequestContext,
    challenge: util.Map[String, String]
  ): Header = {
    val authorization = challenge.get("authorization")
    val resource = challenge.get("resource")
    val clientId = connectionInfo.clientId
    val clientKey = connectionInfo.clientKey
    val token: AuthenticationResult = getAccessTokenFromClientCredentials(
      authorization,
      resource,
      clientId,
      clientKey
    )
    new BasicHeader(
      "Authorization",
      s"""${token.getAccessTokenType} ${token.getAccessToken}"""
    )
  }

  /**
   * Creates the access token
   *
   * @param authorization The authorization from the service
   * @param resource The resource being accessed
   * @param clientId The ClientID for this application
   * @param clientKey The Client Secret for this application
   * @return The access token to use to authenticate to the service
   */
  private def getAccessTokenFromClientCredentials(
    authorization: String,
    resource:      String,
    clientId:      String,
    clientKey:     String
  ): AuthenticationResult = {
    var context: AuthenticationContext = null
    var result: AuthenticationResult = null
    var service: ExecutorService = null
    try {
      service = Executors.newFixedThreadPool(1)
      context = new AuthenticationContext(
        authorization,
        false,
        service
      )
      val credentials: ClientCredential = new ClientCredential(
        clientId,
        clientKey
      )
      val future: Future[AuthenticationResult] = context.acquireToken(
        resource,
        credentials,
        null
      )
      result = future.get()
    } catch {
      case ex: Exception =>
        throw new RuntimeException(ex)
    } finally {
      service.shutdown()
    }

    if (result == null) {
      throw new RuntimeException("authentication result was null")
    }
    result
  }
}
