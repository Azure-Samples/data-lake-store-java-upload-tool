package com.starbucks.analytics.eventhub


import java.util.function.Consumer

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.microsoft.azure.eventprocessorhost.{EventProcessorHost, ExceptionReceivedEventArgs}
import com.microsoft.azure.servicebus.ConnectionStringBuilder
import com.starbucks.analytics.blob.BlobConnectionInfo
import com.typesafe.scalalogging.Logger

import scala.util.Try
import scala.util.control.Breaks._


/**
 * Manages all the Azure Event Hub operations
 */
object EventHubManager {
  private val logger = Logger("EventHubManager")

  /**
   * Loan an Microsoft Azure Event Hub client
   *
   * @param connectionInfo Azure Event Hub Connection Information
   * @param f Function that takes the Azure Data Lake Store
   * @tparam R Type of the return value
   * @return Return value
   */
  def withAzureEventHubClient[R](
    connectionInfo: EventHubConnectionInfo,
    f:              (EventHubClient) => R
  ): Try[R] = {
    val eventHubConnectionString: ConnectionStringBuilder = new ConnectionStringBuilder(
      connectionInfo.eventHubNamespaceName,
      connectionInfo.eventHubName,
      connectionInfo.sasKeyName,
      connectionInfo.sasKey
    )
    val eventHubClient = EventHubClient.createFromConnectionStringSync(
      eventHubConnectionString.toString
    )
    val result = Try(f(eventHubClient))
    result
  }

  /**
   * Publish events to the event hub
   *
   * @param connectionInfo Azure Event Hub Connection Information
   * @param events Events to be published to the event hub
   */
  def publishEvents(
    connectionInfo: EventHubConnectionInfo,
    events:         List[Event]
  ): Try[Boolean] = {
    def fn(eventHubClient: EventHubClient): Boolean = {
      var success = false
      breakable {
        events.foreach(event => {
          logger.debug(s"Publishing event ${event.toJson}" +
            s" to event hub ${connectionInfo.eventHubNamespaceName}/" +
            s" ${connectionInfo.eventHubName}")
          val eventData = new EventData(event.toJson.getBytes("UTF-8"))
          try {
            eventHubClient.sendSync(eventData)
            success = true
          } catch {
            case ex: Exception => {
              logger.error(s"Publishing event ${event.toJson}" +
                s" to event hub ${connectionInfo.eventHubNamespaceName}/" +
                s" ${connectionInfo.eventHubName} failed with exception" +
                s" $ex")
              success = false
              break()
            }
          }
        })
      }
      success
    }
    withAzureEventHubClient[Boolean](
      connectionInfo,
      fn
    )
  }

  /**
    * Method to get Azure EventProcessorHost.
    * @param eventHubConnectionInfo Azure event hub connection information case class.
    * @param blobConnectionInfo Azure blob storage connection information case class.
    * @return
    */
  def getEventProcessorHost(eventHubConnectionInfo: EventHubConnectionInfo, blobConnectionInfo: BlobConnectionInfo): Try[EventProcessorHost] ={
    val storageConnectionString: String = s"DefaultEndpointsProtocol=https;AccountName=${blobConnectionInfo.accountName};AccountKey=${blobConnectionInfo.accountKey};EndpointSuffix=core.windows.net"
    val eventHubConnectionString = new ConnectionStringBuilder(eventHubConnectionInfo.eventHubNamespaceName, eventHubConnectionInfo.eventHubName, eventHubConnectionInfo.sasKeyName, eventHubConnectionInfo.sasKey)
    Try(new EventProcessorHost(eventHubConnectionInfo.eventProcessorName, eventHubConnectionInfo.eventHubName,
      eventHubConnectionInfo.consumerGroup, eventHubConnectionString.toString,
      storageConnectionString, eventHubConnectionInfo.eventProcessorStorageContainer))
  }
}
