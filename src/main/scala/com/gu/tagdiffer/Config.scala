package com.gu.tagdiffer

import com.gu.conf._

object Config {
  val configuration = ConfigurationFactory("tag-differ")

  lazy val dbLocation = configuration("db.location")
  lazy val dbUser = configuration("db.user")
  lazy val dbPassword = configuration("db.password")

  lazy val mongoURI = configuration("mongo.uri")

  lazy val composerContentPrefix = configuration.getStringProperty("composerContentPrefix", "")
}
