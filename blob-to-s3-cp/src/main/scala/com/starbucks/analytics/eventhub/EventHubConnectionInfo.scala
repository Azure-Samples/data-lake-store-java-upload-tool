package com.starbucks.analytics.eventhub

/**
 * Represents the Azure Event Hub connection information
 *
 * @param eventHubNamespaceName Namespace of the event hub
 * @param eventHubName Name of the event hub
 * @param sasKeyName Shared Access Signature key name
 * @param sasKey Shared Access Signature key
 */
case class EventHubConnectionInfo(
                                  eventHubNamespaceName: String,
                                  eventHubName:          String,
                                  sasKeyName:            String,
                                  sasKey:                String,
                                  eventProcessorName:    String,
                                  eventProcessorStorageContainer: String,
                                  consumerGroup:         String
                                 )
