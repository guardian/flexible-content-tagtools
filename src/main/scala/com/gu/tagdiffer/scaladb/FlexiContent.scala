package com.gu.tagdiffer.scaladb

import java.util.NoSuchElementException

import com.gu.tagdiffer.index.model.`package`.ContentId
import com.gu.tagdiffer.index.model.{Tagging, Section, TagType, Tag}
import org.joda.time.DateTime
import reactivemongo.bson.{BSONArray, BSONDateTime, BSONDocument, BSONDocumentReader}

import scala.util.control.NonFatal

case class FlexiContent(
  contentId: ContentId,
  pageId: Option[String],
  contentType: String,
  created: DateTime,
  lastModified: DateTime,
  mainTags: Option[List[Tagging]],
  contributorTags: Option[List[Tagging]],
  publicationTags: Option[List[Tagging]],
  bookTags: Option[List[Tagging]],
  sectionTags: Option[List[Tagging]]
)

object FlexiContent {
  implicit object ComposerContentBSONRreader extends BSONDocumentReader[FlexiContent] {
    def read(doc: BSONDocument): FlexiContent = {

      val contentId = doc.getAs[ContentId]("_id").get
      try {
        val taxonomy = doc.getAs[BSONDocument]("taxonomy")
        val newspaper = taxonomy.flatMap(_.getAs[BSONDocument]("newspaper"))
        val contentChangeDetails = doc.getAs[BSONDocument]("contentChangeDetails").flatMap(c => c.getAs[BSONDocument]("created")).get
        FlexiContent(
        contentId,
        doc.getAs[BSONDocument]("identifiers").flatMap(_.getAs[String]("pageId")).map(i => i),
        doc.getAs[String]("type").get,
        {
          val timestamp = contentChangeDetails.getAs[BSONDateTime]("date").get
          new DateTime(timestamp.value)
        }, {
          val timestamp = contentChangeDetails.getAs[BSONDateTime]("date").get
          new DateTime(timestamp.value)
        }, {
          taxonomy.flatMap(_.getAs[BSONArray]("tags")).map { bsArray =>
            bsArray.values.toList.flatMap {
              case v: BSONDocument =>
                val tag = v.getAs[BSONDocument]("tag").get
                val section = tag.getAs[BSONDocument]("section").get
                val isLead = v.getAs[Boolean]("isLead").get

                extractTagInformation(tag, section, isLead)
            }
          }
        }, {
          taxonomy.flatMap(_.getAs[BSONArray]("contributors")).map { bsArray =>
            bsArray.values.toList.flatMap {
              case v: BSONDocument =>
                val section = v.getAs[BSONDocument]("section").get
                extractTagInformation(v, section)
            }
          }
        }, {
          taxonomy.flatMap(_.getAs[BSONDocument]("publication")).map { pub =>
            List(extractTagInformation(pub, pub.getAs[BSONDocument]("section").get).get)
          }
        }, {
          newspaper.flatMap(_.getAs[BSONDocument]("book")).map { book =>
            List(extractTagInformation(book, book.getAs[BSONDocument]("section").get).get)
          }
        }, {
          newspaper.flatMap(_.getAs[BSONDocument]("bookSection")).map { book =>
            List(extractTagInformation(book, book.getAs[BSONDocument]("section").get).get)
          }
        }
        )
      } catch {
        case NonFatal(e) =>
          System.err.println(s"FlexiContent parser throwing exception whilst parsing $contentId: ${e.getMessage}")
          e.printStackTrace(System.err)
          throw e
      }
    }

    private def extractTagInformation(tag: BSONDocument, section: BSONDocument, isLead: Boolean = false): Option[Tagging] = {
      val tagId = tag.getAs[Long]("id").get
      val tagType = tag.getAs[String]("type").get
      val tt = try {
        TagType.withName(tagType)
      } catch {
        case e:NoSuchElementException => TagType.Other
      }
      val internalName = tag.getAs[String]("internalName").get
      val externalName = tag.getAs[String]("externalName").get
      val slug = tag.getAs[String]("slug")
      // Section
      val sectionId = section.getAs[Long]("id").get
      val sectionName = section.getAs[String]("name").get
      val sectionPathPrefix = section.getAs[String]("pathPrefix")
      val sectionSlug= section.getAs[String]("slug").get
      val sec = Section(sectionId, sectionName, sectionPathPrefix, sectionSlug)

      Some(Tagging(Tag.createFromFlex(tagId, tt, internalName, externalName, slug, sec), isLead))
    }
  }
}
