package com.starbucks.analytics.eventhub

class Event(uri: String, sharedAccessSignatureToken: String) {
  def toJson: String =
    s"""{
       |    "uri": "$uri",
       |    "sharedAccessSignatureToken": "$sharedAccessSignatureToken"
       |}
     """.stripMargin
}
