package com.gu.tagdiffer.scaladb

import java.sql.{PreparedStatement, Connection}

/**
 * Represents a query that can be executed
 *
 * A query will always have a sql string, and may have parameters.
 */
trait Query {

  def parameters: Vector[Any]

  def sql: String

  def param(value: Any): Query

  private[scaladb] final def mkStatement(con: Connection): PreparedStatement = {
    val statement = con.prepareStatement(sql)

    parameters.zipWithIndex.foreach { case (p, idx) =>
      statement.setObject(idx + 1, p)
    }

    statement
  }

  protected final def paramsToString = parameters.mkString("(", ", ", ")")

}

object Query {

  def apply(sql: String, parameters: Vector[Any] = Vector.empty): Query =
    new ParameterisedQuery(sql, parameters)

}

case class ParameterisedQuery(sql: String, parameters: Vector[Any]) extends Query {

  def param(value: Any) = copy(parameters = parameters :+ value)

  override def toString = sql + " " + paramsToString

}
