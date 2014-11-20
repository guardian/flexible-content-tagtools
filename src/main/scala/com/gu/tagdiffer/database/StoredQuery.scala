package com.gu.tagdiffer.database

import io.Source
import com.gu.tagdiffer.scaladb.Query
import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._

object StoredQuery {

  val loadedSql = new ConcurrentHashMap[String, String]()

  def loadFile(fileName: String) = {
    loadedSql.getOrElseUpdate(fileName, {
      val stream = getClass.getResourceAsStream("/" + fileName)
      require(stream != null, "Could not find classpath resource: " + fileName)
      val sql = Source.fromInputStream(stream).mkString
      stream.close
      sql
    })
  }

  def loadFile(fileName: Option[String]): String =
    fileName.map(loadFile(_)).getOrElse("")

  def fromClasspath(queryFilename: String) =
    LoadedQuery(queryFilename, None)

  case class LoadedQuery(sql: String, parameters: Vector[Any], templateFilename: String, subQueryFilename: Option[String]) extends Query {

    def param(value: Any) = copy(parameters = parameters :+ value)

    override def toString = templateFilename + subQueryFilename.map("+" + _).getOrElse("") + paramsToString

  }

  object LoadedQuery {
    def apply(templateFilename: String, subQueryFilename: Option[String] = None): LoadedQuery =
      LoadedQuery(
        buildSql(templateFilename, subQueryFilename),
        Vector.empty,
        tidy(templateFilename),
        subQueryFilename.map(tidy(_))
      )

    def buildSql(templateFilename: String, subQueryFilename: Option[String]) =
      loadFile(templateFilename).replace("!SUBQUERY!", loadFile(subQueryFilename))

    def tidy(filename: String) = filename.replace("sql", "").replace(".sql", "")

  }
}
