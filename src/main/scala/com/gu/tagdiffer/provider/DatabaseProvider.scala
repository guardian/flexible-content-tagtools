package com.gu.tagdiffer.provider

import com.gu.tagdiffer.scaladb.Database

trait DatabaseProvider {
  def database: Database
}
