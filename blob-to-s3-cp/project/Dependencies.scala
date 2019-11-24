import sbt._

object Dependencies {
  // Compile scope:
  // Scope available in all classpath, transitive by default.
  lazy val scala_logging: ModuleID = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  lazy val logback_classic: ModuleID = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val scallop: ModuleID = "org.rogach" %% "scallop" % "3.0.3"
  lazy val joda_time: ModuleID = "joda-time" % "joda-time" % "2.9.9"
  lazy val azure_data_lake_store_sdk: ModuleID = "com.microsoft.azure" % "azure-data-lake-store-sdk" % "2.1.5"
  lazy val azure_storage_sdk: ModuleID = "com.microsoft.azure" % "azure-storage" % "5.3.1"
  lazy val azure_eventhubs_sdk: ModuleID = "com.microsoft.azure" % "azure-eventhubs" % "0.14.0"
  lazy val azure_eventhubs_eph: ModuleID = "com.microsoft.azure" % "azure-eventhubs-eph" % "0.14.0"
  lazy val aws_s3_sdk: ModuleID = "com.amazonaws" % "aws-java-sdk" % "1.11.162"
  lazy val azure_keyvault_sdk: ModuleID = "com.microsoft.azure" % "azure-keyvault" % "0.9.7"
  lazy val azure_keyvault_extensions_sdk: ModuleID = "com.microsoft.azure" % "azure-keyvault-extensions" % "0.9.7"

  // Provided scope:
  // Scope provided by container, available only in compile and test classpath, non-transitive by default.

  // Runtime scope:
  // Scope provided in runtime, available only in runtime and test classpath, not compile classpath, non-transitive by default.

  // Test scope:
  // Scope available only in test classpath, non-transitive by default.
}