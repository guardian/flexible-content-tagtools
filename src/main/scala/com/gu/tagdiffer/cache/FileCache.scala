package com.gu.tagdiffer.cache

import java.io.{PrintWriter, ObjectInputStream}
import java.util.NoSuchElementException

import com.gu.tagdiffer.index.model._
import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model.ContentCategory._
import com.gu.tagdiffer.r2.R2
import play.api.libs.json._

import scala.io.Source
import scala.util.control.NonFatal

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}


object FileCache {
  implicit val ContentCategoryFormats = new Format[Category] {
    def reads(json: JsValue): JsResult[Category] = json match {
      case JsString(value) =>
        try {
          val enum = ContentCategory.withName(value)
          JsSuccess(enum)
        } catch {
          case e:NoSuchElementException =>
            JsError(s"Invalid enum value: $value")
        }
      case _ => JsError(s"Invalid type for enum value (expected JsString)")
    }

    def writes(o: Category): JsValue = JsString(o.toString)
  }

  implicit val TagTypeFormats = new Format[TagType] {
    def reads(json: JsValue): JsResult[TagType] = json match {
      case JsString(value) =>
        try {
          val enum = TagType.withName(value)
          JsSuccess(enum)
        } catch {
          case e:NoSuchElementException =>
            JsError(s"Invalid enum value: $value")
        }
      case _ => JsError(s"Invalid type for enum value (expected JsString)")
    }

    def writes(o: TagType): JsValue = JsString(o.toString)
  }

  def serializeR2CacheToDisk(): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val r2CacheAsJson = write(R2.cache)
    val file = s"r2cache.cache"
    println("Writing r2 cache to disk")
    val writer = new PrintWriter(file)
    writer.println(r2CacheAsJson)
    writer.close()
    println("Finished caching r2 to disk")
  }

  def readR2CacheFromDisk: Iterator[R2] = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val file = s"r2cache.cache"
    val r2CacheAsJson = Source.fromFile(file).getLines()
    println("No of lines: " + r2CacheAsJson.size)
    val r2 = r2CacheAsJson.map(r2c => read[R2](r2c))
    r2
  }


  implicit val SectionFormats = Json.format[Section]
  implicit val TagFormats = Json.format[Tag]
  implicit val TaggingFormats = Json.format[Tagging]
  implicit val R2TagsFormats = Json.format[R2Tags]
  implicit val FlexiTagsFormats = Json.format[FlexiTags]
  implicit val ContentFormats = Json.format[Content]

  // Write data to cache file as a Json
  def writeContentToDisk(content: Map[Category, List[Content]], filePrefix: String): Unit = {
    content.foreach { case (key, value) => key.toString -> value
      val file = s"$filePrefix-$key.cache"
      System.err.println(s"Writing content to $file")
      val writer = new PrintWriter(file)
      value.foreach { item =>
        writer.println(Json.stringify(Json.toJson(item)))
      }
      writer.close()
      System.err.println(s"Finished writing content to $file")
    }
  }
  // Get data from cache file
  def sourceContentFromDisk(filePrefix: String): Map[Category, List[Content]] = {
    val categories: Set[Category] = ContentCategory.values.toSet
    categories.map { category =>
      val file = s"$filePrefix-${category.toString}.cache"
      System.err.println(s"Attempting to read and parse content from $file")
      val contentLines = Source.fromFile(file).getLines()
      val content = contentLines.flatMap { line =>
        try {
          Json.fromJson[Content](Json.parse(line)).asOpt
        } catch {
          case NonFatal(e) => {
            None
          }
        }
      }
      val contentList = content.toList
      System.err.println(s"Successfully read and parsed content from $file")
      category -> contentList
    }.toMap
  }
}
