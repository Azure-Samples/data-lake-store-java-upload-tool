package com.starbucks.analytics.eventhub

import java.util.function.Consumer

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs
import com.starbucks.analytics.eventhub.EventHubManager.logger
import com.typesafe.scalalogging.Logger

/**
  * Error Handler class for errors thrown by Event Processor Host.
  */
class EventHubErrorNotificationHandler extends Consumer[ExceptionReceivedEventArgs] {
  private val logger = Logger("EventHubProcessorErrorHandler")

  override def accept(t: ExceptionReceivedEventArgs): Unit = {
    logger.error("Host "+ t.getHostname + " received general error notification during "+ t.getAction+ ": "+ t.getException.toString)
  }
}
