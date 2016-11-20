package com.microsoft.azure.adls

import java.sql.Timestamp

import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import slick.driver.H2Driver.api._

import scala.concurrent.Await
import scala.concurrent.duration._

object StateSchema {

  import scala.concurrent.ExecutionContext.Implicits.global

  object Status extends Enumeration {
    type Status = Value
    val Init = Value("Initialized")
    val InProgress = Value("InProgress")
    val Succeeded = Value("Succeeded")
    val Failed = Value("Failed")
  }

  import Status._

  final case class State(
                          sourceTableName: String,
                          partition: Option[String],
                          query: String,
                          destinationFolder: String,
                          lastTransferredDateTime: Option[DateTime],
                          status: Status
                        )

  final class StateTable(tag: Tag)
    extends Table[State](tag, "STATES") {
    def sourceTableName = column[String]("tableName")

    def partition = column[Option[String]]("partition")

    def query = column[String]("query")

    def destinationFolder = column[String]("destinationFolder")

    def lastTransferredDateTime = column[Option[DateTime]]("lastTransferredDateTime")

    def status = column[Status]("status")

    def * = (sourceTableName, partition, query, destinationFolder, lastTransferredDateTime, status) <>
      (State.tupled, State.unapply)
  }

  implicit val jodaDateTimeType =
    MappedColumnType.base[DateTime, Timestamp](
      dt => new Timestamp(dt.getMillis),
      ts => new DateTime(ts.getTime, UTC))

  implicit val StatusMapper = MappedColumnType.base[Status, String](
    e => e.toString,
    s => Status.withName(s))

  lazy val state = TableQuery[StateTable]

  // Create an in-memory H2 database;
  val db = Database.forConfig("dataTransfer")

  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), 2 seconds)

  def init(): Unit = {
    exec(state.schema.create)
  }

  def insert(data: Seq[State]): Unit = {
    exec(state ++= data)
  }

  def print(): Unit = {
    exec(state.result) foreach {
      println
    }
  }
}
