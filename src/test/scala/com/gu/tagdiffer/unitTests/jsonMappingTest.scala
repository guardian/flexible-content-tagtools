package com.gu.tagdiffer.unitTests

import com.gu.tagdiffer.TagDiffer
import com.gu.tagdiffer.TagDiffer.{CSVFileResult, JSONFileResult}
import com.gu.tagdiffer.cache.FileCache
import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model._
import org.joda.time.DateTime
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.scalatest.matchers.ShouldMatchers
import play.api.libs.json._
import play.api.libs.functional.syntax._
import com.gu.tagdiffer.unitTests.TestTags._

import scala.util.parsing.json.JSONObject

case class TagMapping (pageId: String,
                       contentId: ContentId,
                       lastModifiedFlexi: DateTime,
                       lastModifiedR2: DateTime,
                       tags: List[Tagging],
                       contributors: List[Tagging],
                       publication: Tagging,
                       book: Tagging,
                       bookSection: Tagging)

// Utiliry object to read JSON values as Enum values
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

class JsonMappingTest extends FeatureSpec with GivenWhenThen with ShouldMatchers {
  // JSON Reads and Format
  implicit val SectionFormats = FileCache.SectionFormats
  implicit val TagTypeFormat: Reads[TagType.Value] = EnumUtils.enumReads(TagType)
  implicit val TagFormats: Reads[Tag] = (
    (__ \ "id").read[Long] and
      (__ \ "type").read[TagType] and
      (__ \ "internalName").read[String] and
      (__ \ "externalName").read[String] and
      (__ \ "section").read[Section] and
      (__ \ "existInR2").readNullable[Boolean]
    )(Tag.apply _)
  implicit val TaggingFormats: Reads[Tagging] = (
    (__ \ "tag").read[Tag] and
      (__ \ "isLead").readNullable[Boolean].map(_.getOrElse(false))
    )(Tagging.apply _)
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
    )(TagMapping.apply _)

  feature("The json format is valid") {
    scenario("differences exist") {
      val flexiTags = FlexiTags(List(leadTag, newTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, mainTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).foreach {
        case JSONFileResult(filename, jsonList) =>
          val json = jsonList.head
          then("the json has the expected format")
          val res = json.validate[TagMapping]
          res.isSuccess should be(true)
          and("and tags are placed in the right section according to their type")
          val mapping = res.get
          val isTagTypeCorrect = (mapping.book.tagType == Book) && (mapping.bookSection.tagType == BookSection) &&
            (mapping.contributors.forall(_.tagType == Contributor)) && (mapping.publication.tagType == Publication) &&
            (mapping.tags.forall(_.tagType == Other))
          isTagTypeCorrect should be(true)
          and ("the order of shared main tags is preserved and different tags are added at the bottom")
          val mainTags = (mapping.tags.head.tagId == leadTag.tagId) && (mapping.tags.last.tagId == mainTag.tagId)
          mainTags should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }

    scenario("differences do not exist") {
      val flexiTags = FlexiTags(List(mainTag, leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).foreach {
        case JSONFileResult(filename, jsonList) =>
          then("the list of json mappings should be empty")
          jsonList.isEmpty should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }
  }

  feature("Newspaper tag duplication") {
    scenario("flexible-content has different main tags and duplicated newspaper tags") {
      val flexiTags = FlexiTags(List(leadTag, bookTag, bookSectionTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).foreach {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val mainTags = res.tags.forall(t => (t.tagType != Book) && (t.tagType != BookSection)) &&
            res.tags.filter(_.tagId == mainTag.tagId).nonEmpty
          val newspaperTags = (res.book.tagId == bookTag.tagId) && (res.bookSection.tagId == bookSectionTag.tagId)
          then("the newspaper tag should not appear in mapping main tag list")
          mainTags should be(true)
          and("should only appear in the mapping newspaper section")
          newspaperTags should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }

    scenario("flexible-content has only duplicated newspaper tags ") {
      val flexiTags = FlexiTags(List(leadTag, bookTag, bookSectionTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).foreach {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val mainTags = res.tags.forall(t => (t.tagType != Book) && (t.tagType != BookSection))
          val newspaperTags = (res.book.tagId == bookTag.tagId) && (res.bookSection.tagId == bookSectionTag.tagId)
          then("newspaper tag should not appear between mapping main tag")
          mainTags should be(true)
          and("should only appear in the mapping newspaper section")
          newspaperTags should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }
  }

  feature("Contributor tag discrepancy") {
    scenario("flexible-content has no contributor tag") {
      val flexiTags = FlexiTags(List(leadTag), List(), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val contributor = (res.contributors.length == 1) && (res.contributors.head.tagId == contributorTag.tagId)
          then("the R2 contributor tag is the mapping contributor tag")
          contributor should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }

    scenario("flexible-content and R2 have different contributor tags") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag2, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val contributor = (res.contributors.length == 2)
          then("both tags are present in the mapping")
          contributor should be(true)
          and("the flexible content contributor tag is the first one")
          (res.contributors.head.tagId == contributorTag.tagId) &&
            (res.contributors.last.tagId == contributorTag2.tagId) should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }
  }

  feature("R2 has multiple publication tags") {
    scenario("Flexible-content has no publication tag") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, publicationTag2, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val mainTags = res.tags.filter(_.tagId == publicationTag2.tagId).nonEmpty
          val publication = (res.publication.tagId == publicationTag.tagId)
          then("the first R2 publication tag is the mapping publication tag")
          publication should be(true)
          and("the extra publication tag is included in the mapping main tags")
          mainTags should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }

    scenario("R2 and flexible-content share one publication tag") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, publicationTag2, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val mainTags = res.tags.filter(_.tagId == publicationTag2.tagId).nonEmpty
          val publication = (res.publication.tagId == publicationTag.tagId)
          then("the shared publication tag is the mapping's publication tag")
          publication should be(true)
          and("the extra publication tag is included between the mapping main tags")
          mainTags should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }
  }

  feature("Lead tag discrepancy") {
    scenario("the same tag is lead tag in flexible-content but not in R2") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(noLeadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val leadtag = (res.tags.length == 1) && res.tags.head.isLead
          then("the mapping has only the lead tag")
          leadtag should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }

    scenario("the same tag is lead tag in R2 but not in flexible-content") {
      val flexiTags = FlexiTags(List(noLeadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val leadtag = (res.tags.length == 1) && res.tags.head.isLead
          then("the mapping has only the lead tag")
          leadtag should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }
  }

  feature("Tag renaming/migration") {
    scenario("the tag has been renamed in R2 but keeps the same ID") {
      val flexiTags = FlexiTags(List(mainTag, noLeadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTagRenamed, noLeadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, Map.empty[Long, Long]).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val renamed = (res.tags.length == 2) && (res.tags.exists(_.internalName.contentEquals(mainTagRenamed.internalName)))
          then("only the renamed tag is in the mapping")
          renamed should be(true)
          and("the original order is preserved")
          (res.tags.head.internalName == mainTagRenamed.internalName) should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }

    scenario("the tag ID has changed after section migration") {
      val flexiTags = FlexiTags(List(oldTag, noLeadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(newTag, noLeadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      val oldToNewTagId = Map(oldTag.tagId -> newTag.tagId)

      val mapContentCategory = Map(ContentCategory.Live -> List[Content](content))
      TagDiffer.correctTagMapping.compare(mapContentCategory, oldToNewTagId).map {
        case JSONFileResult(filename, jsonList) =>
          val res = jsonList.head.validate[TagMapping].get
          val migrated = (res.tags.length == 2) && (res.tags.exists(_.tagId == newTag.tagId))
          then("only the new tag is in the mapping")
          migrated should be(true)
          and("the original order is preserved")
          (res.tags.head.tagId == newTag.tagId) should be(true)
        case CSVFileResult(filename, header, lines) => None
      }
    }
  }
}

