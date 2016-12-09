import sbt._

object Dependencies {
  // Compile scope:
  // Scope available in all classpath, transitive by default.
  lazy val slf4j_api = "org.slf4j" % "slf4j-api" % "1.7.21"
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.1.7"
  lazy val scopt = "com.github.scopt" %% "scopt" % "3.5.0"
  lazy val azure_data_lake_store_sdk = "com.microsoft.azure" % "azure-data-lake-store-sdk" % "2.0.4-SNAPSHOT"

  // Provided scope:
  // Scope provided by container, available only in compile and test classpath, non-transitive by default.

  // Runtime scope:
  // Scope provided in runtime, available only in runtime and test classpath, not compile classpath, non-transitive by default.

  // Test scope:
  // Scope available only in test classpath, non-transitive by default.
}