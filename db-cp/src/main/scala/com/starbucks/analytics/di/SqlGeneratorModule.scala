package com.starbucks.analytics.di

import com.google.inject.AbstractModule
import com.starbucks.analytics.db.Oracle.OracleSqlGenerator
import com.starbucks.analytics.db.SqlGenerator
import com.starbucks.analytics.db.SqlServer.SqlServerSqlGenerator
import net.codingwell.scalaguice.{ScalaMapBinder, ScalaModule}

class SqlGeneratorModule extends AbstractModule with ScalaModule {
  def configure(): Unit = {
    val mapBinder = ScalaMapBinder.newMapBinder[String, SqlGenerator](binder)
    mapBinder.addBinding("ORACLESQLGENERATOR").toInstance(OracleSqlGenerator)
    mapBinder.addBinding("SQLSERVERSQLGENERATOR").toInstance(SqlServerSqlGenerator)
  }
}