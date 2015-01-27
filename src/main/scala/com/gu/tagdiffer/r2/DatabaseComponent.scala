package com.gu.tagdiffer.r2

import java.util.Properties
import javax.sql.DataSource

import com.gu.tagdiffer.Config
import com.gu.tagdiffer.r2.DatabaseProvider
import oracle.jdbc.pool.OracleDataSource

trait DatabaseComponent extends DatabaseProvider {

  lazy val database = new Database(dataSource)

  private def dataSource: DataSource = {
    val oracleDataSource = new OracleDataSource

    oracleDataSource.setURL(Config.dbLocation)
    oracleDataSource.setUser(Config.dbUser)
    oracleDataSource.setPassword(Config.dbPassword)

    val properties = new Properties
    properties.put("v$session.program", "tag-differ")
    oracleDataSource.setConnectionProperties(properties)

    oracleDataSource.setConnectionCachingEnabled(true)
    val cacheProps: Properties = new Properties
    cacheProps.setProperty("InactivityTimeout", "60")
    cacheProps.setProperty("PropertyCheckInterval", "60")
    oracleDataSource.setConnectionCacheProperties(cacheProps)

    oracleDataSource
  }
}
