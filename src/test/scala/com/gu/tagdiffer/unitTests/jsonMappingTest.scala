package com.gu.tagdiffer.unitTests

import com.gu.tagdiffer.TagDiffer
import com.gu.tagdiffer.index.model
import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model._
import org.joda.time.DateTime
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.scalatest.matchers.ShouldMatchers
import play.api.data.validation.ValidationError
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class TagMapping (pageId: String,
                       contentId: ContentId,
                       lastModifiedFlexi: DateTime,
                       lastModifiedR2: DateTime,
                        tags: List[Tagging],
                        contributors: List[Tagging],
                        publication: Tagging,
                        book: Tagging,
                        bookSection: Tagging)

object EnumUtils {
  def enumReads[E <: Enumeration](enum: E): Reads[E#Value] = new Reads[E#Value] {
    def reads(json: JsValue): JsResult[E#Value] = json match {
      case JsString(s) => {
        try {
          JsSuccess(enum.withName(s))
        } catch {
          case _: NoSuchElementException => JsError(s"Enumeration expected of type: '${enum.getClass}', but it does not appear to contain the value: '$s'")
        }
      }
      case _ => JsError("String value expected")
    }
  }
}

class jsonMappingTest extends FeatureSpec with GivenWhenThen with ShouldMatchers {
  // Create Tags
  val section = Section(8, "test section", Some("testSectionPath"), "section")
  val mainTag = Tagging(Tag(1, Other, "tag 2", "tag", section, Some(true)), false)
  val leadTag = Tagging(Tag(2, Other, "tag 3", "tag", section, Some(true)), true)
  val noLeadTag = Tagging(Tag(2, Other, "tag 3", "tag", section, Some(true)), false)
  val contributorTag = Tagging(Tag(3, Contributor, "tag 4", "tag", section, Some(true)), false)
  val contributorTag2 =Tagging( Tag(4, Contributor, "tag 5", "tag", section, Some(true)), false)
  val publicationTag = Tagging(Tag(5, Publication, "tag 6", "tag", section, Some(true)), false)
  val bookTag = Tagging(Tag(6, Book, "tag 7", "tag", section, Some(true)), false)
  val bookSectionTag = Tagging(Tag(7, BookSection, "tag 8", "tag", section, Some(true)), false)
  val createTimestamp = new DateTime()
  val lastModified = new DateTime()

  // init tag migration tag
  TagDiffer.tagMigrationCache = Map.empty[Long, Tagging]

  // read json
  implicit val SectionFormats = TagDiffer.SectionFormats
  implicit val TagTypeFormat: Reads[TagType.Value] = EnumUtils.enumReads(TagType)

  implicit val TagFormats: Reads[Tag] = (
    (__ \ "tagId").read[Long] and
      (__ \ "tagType").read[TagType] and
      (__ \ "internalName").read[String] and
      (__ \ "externalName").read[String] and
      (__ \ "section").read[Section] and
      (__ \ "existInR2").readNullable[Boolean]
    ) (Tag.apply _)

  implicit val TaggingFormats = TagDiffer.TaggingFormats
  implicit val TagMappingReader: Reads[TagMapping] = (
    (__ \ "pageId").read[String] and
      (__ \ "contentId").read[ContentId] and
      (__ \ "lastModifiedFlexi").read[DateTime] and
      (__ \ "lastModifiedR2").read[DateTime] and
      (__ \ "taxonomy" \\ "tags").read[List[Tagging]] and
      (__ \ "taxonomy" \\ "contributors").read[List[Tagging]] and
      (__ \ "taxonomy" \ "publication").read[Tagging] and
      (__ \ "taxonomy" \ "newspaper" \ "book").read[Tagging] and
      (__ \ "taxonomy" \ "newspaper" \ "bookSection").read[Tagging]
    ) (TagMapping.apply _)

  feature("The json has the valid structure") {
    scenario ("differences exist"){
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag,  contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      then("the json of tag mapping should be the one expected")
      val res = jsonMapping.validate[TagMapping]

      res.isSuccess should be(true)
    }

    scenario ("difference do not exists"){
      val flexiTags = FlexiTags(List(mainTag, leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag,  contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content))
      then("the list of tag mapping json should be empty")
      jsonMapping.isEmpty should be(true)
    }
  }

}
