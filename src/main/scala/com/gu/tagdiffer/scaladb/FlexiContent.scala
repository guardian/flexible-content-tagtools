package com.gu.tagdiffer.scaladb

import java.util.NoSuchElementException

import com.gu.tagdiffer.index.model.{TagType, Tag}
import org.joda.time.DateTime
import reactivemongo.bson.{BSONArray, BSONDateTime, BSONDocument, BSONDocumentReader}

import scala.util.control.NonFatal

case class FlexiContent(
  contentId: String,
  pageId: Option[String],
  contentType: String,
  date: DateTime,
  mainTags: Option[List[Tag]],
  contributorTags: Option[List[Tag]],
  publicationTags: Option[List[Tag]],
  bookTags: Option[List[Tag]],
  sectionTags: Option[List[Tag]]
)

object FlexiContent {
  implicit object ComposerContentBSONRreader extends BSONDocumentReader[FlexiContent] {
    def read(doc: BSONDocument): FlexiContent = {
      try {
        val taxonomy = doc.getAs[BSONDocument]("taxonomy")
        val newspaper = taxonomy.flatMap(_.getAs[BSONDocument]("newspaper"))
        FlexiContent(
        doc.getAs[String]("_id").get,
        doc.getAs[BSONDocument]("identifiers").flatMap(_.getAs[String]("pageId")).map(i => i),
        doc.getAs[String]("type").get, {
          val ccd = doc.getAs[BSONDocument]("contentChangeDetails").flatMap(c => c.getAs[BSONDocument]("created")).get
          val timestamp = ccd.getAs[BSONDateTime]("date").get
          new DateTime(timestamp.value)
        }, {
          taxonomy.flatMap(_.getAs[BSONArray]("tags")).map { bsArray =>
            bsArray.values.toList.flatMap {
              case v: BSONDocument =>
                val tag = v.getAs[BSONDocument]("tag").get
                val isLead = v.getAs[Boolean]("isLead").get
                val tagType = tag.getAs[String]("type").get
                val tt = try {
                  TagType.withName(tagType)
                } catch {
                  case e:NoSuchElementException => TagType.Other
                }
                Some(Tag.createFromFlex(tag.getAs[Long]("id").get, tag.getAs[String]("internalName").get, isLead, tt))
            }
          }
        }, {
          taxonomy.flatMap(_.getAs[BSONArray]("contributors")).map { bsArray =>
            bsArray.values.toList.flatMap {
              case v: BSONDocument =>
                Some(Tag.createFromFlex(v.getAs[Long]("id").get, v.getAs[String]("internalName").get, false, TagType.Contributor))
            }
          }
        }, {
          taxonomy.flatMap(_.getAs[BSONDocument]("publication")).map { pub =>
            List(Tag.createFromFlex(pub.getAs[Long]("id").get, pub.getAs[String]("internalName").get, false, TagType.Publication))
          }
        }, {
          newspaper.flatMap(_.getAs[BSONDocument]("book")).map { book =>
            List(Tag.createFromFlex(book.getAs[Long]("id").get, book.getAs[String]("internalName").get, false, TagType.Book))
          }
        }, {
          newspaper.flatMap(_.getAs[BSONDocument]("bookSection")).map { book =>
            List(Tag.createFromFlex(book.getAs[Long]("id").get, book.getAs[String]("internalName").get, false, TagType.BookSection))
          }
        }
        )
      } catch {
        case NonFatal(e) =>
          System.err.println(s"FlexiContent parser throwing exception: ${e.getMessage}")
          e.printStackTrace(System.err)
          throw e
      }
    }
  }
}
