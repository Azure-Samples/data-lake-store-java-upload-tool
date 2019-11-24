package com.starbucks.analytics.eventhub

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient }
import com.microsoft.azure.servicebus.ConnectionStringBuilder
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
  private def withAzureEventHubClient[R](
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
}
