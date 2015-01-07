package com.gu.tagdiffer.unitTests

import com.gu.tagdiffer.TagDiffer
import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model._
import org.joda.time.DateTime
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.scalatest.matchers.ShouldMatchers
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
  val mainTag = Tagging(Tag(1, Other, "tag 1", "tag", section, Some(true)), false)
  val mainTagRenamed = Tagging(mainTag.tag.copy(internalName = "tag renamed"), false)
  val oldTag = Tagging(mainTag.tag.copy(existInR2 = Some(false)), false)
  val newTag = Tagging(oldTag.tag.copy(tagId = 9L, section = section.copy(id = 9L)), false)
  val leadTag = Tagging(Tag(2, Other, "tag 2", "tag", section, Some(true)), true)
  val noLeadTag = leadTag.copy(isLead = false)
  val contributorTag = Tagging(Tag(3, Contributor, "tag 3", "tag", section, Some(true)), false)
  val contributorTag2 = Tagging(Tag(4, Contributor, "tag 4", "tag", section, Some(true)), false)
  val publicationTag = Tagging(Tag(5, Publication, "tag 5", "tag", section, Some(true)), false)
  val publicationTag2 = Tagging(Tag(6, Publication, "tag 6", "tag", section, Some(true)), false)
  val bookTag = Tagging(Tag(7, Book, "tag 7", "tag", section, Some(true)), false)
  val bookSectionTag = Tagging(Tag(8, BookSection, "tag 8", "tag", section, Some(true)), false)
  val createTimestamp = new DateTime()
  val lastModified = new DateTime()

  // init tag migration tag
  TagDiffer.tagMigrationCache = Map(oldTag.tagId -> newTag)

  // JSON Reads and Format
  implicit val SectionFormats = TagDiffer.SectionFormats
  implicit val TagTypeFormat: Reads[TagType.Value] = EnumUtils.enumReads(TagType)
  implicit val TagFormats: Reads[Tag] = (
    (__ \ "tagId").read[Long] and
      (__ \ "tagType").read[TagType] and
      (__ \ "internalName").read[String] and
      (__ \ "externalName").read[String] and
      (__ \ "section").read[Section] and
      (__ \ "existInR2").readNullable[Boolean]
    )(Tag.apply _)
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
    )(TagMapping.apply _)

  feature("The json format is valid") {
    scenario("differences exist") {
      val flexiTags = FlexiTags(List(leadTag, newTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, mainTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      then("the json should be the one expected")
      val res = jsonMapping.validate[TagMapping]
      res.isSuccess should be(true)
      and("each json path contains the expected tagType")
      val mapping = res.get
      val isTagTypeCorrect = (mapping.book.tagType == Book) && (mapping.bookSection.tagType == BookSection) &&
        (mapping.contributors.forall(_.tagType == Contributor)) && (mapping.publication.tagType == Publication) &&
        (mapping.tags.forall(_.tagType == Other))
      isTagTypeCorrect should be(true)
      and ("the original order of main tags is preserved with diff tags added at the bottom")
      val mainTags = (mapping.tags.head.tagId == leadTag.tagId) && (mapping.tags.last.tagId == mainTag.tagId)
    }

    scenario("differences do not exist") {
      val flexiTags = FlexiTags(List(mainTag, leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content))
      then("the list of json should be empty")
      jsonMapping.isEmpty should be(true)
    }
  }

  feature("Newspaper tag duplication") {
    scenario("Flexible-content has differences and duplicated newspaper tags ") {
      val flexiTags = FlexiTags(List(leadTag, bookTag, bookSectionTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val mainTags = res.tags.forall(t => (t.tagType != Book) && (t.tagType != BookSection)) &&
        res.tags.filter(_.tagId == mainTag.tagId).nonEmpty
      val newspaperTags = (res.book.tagId == bookTag.tagId) && (res.bookSection.tagId == bookSectionTag.tagId)
      then("newspaper tag should not appear in main tag list")
      mainTags should be(true)
      and("should only be in the newspaper section")
      newspaperTags should be(true)
    }

    scenario("Flexible-content has only duplicated newspaper tags ") {
      val flexiTags = FlexiTags(List(leadTag, bookTag, bookSectionTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val mainTags = res.tags.forall(t => (t.tagType != Book) && (t.tagType != BookSection))
      val newspaperTags = (res.book.tagId == bookTag.tagId) && (res.bookSection.tagId == bookSectionTag.tagId)
      then("newspaper tag should not appear in main tag list")
      mainTags should be(true)
      and("should only be in the newspaper section")
      newspaperTags should be(true)
    }
  }

  feature("Contributor tag discrepancy") {
    scenario("Flexible-content has no contributor tag") {
      val flexiTags = FlexiTags(List(leadTag), List(), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val contributor = (res.contributors.length == 1) && (res.contributors.head.tagId == contributorTag.tagId)
      then("the R2 contributor tag is the new contributor tag")
      contributor should be(true)
    }

    scenario("Flexible-content have different contributor tags") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag2, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val contributor = (res.contributors.length == 2) && (res.contributors.head.tagId == contributorTag.tagId) &&
        (res.contributors.last.tagId == contributorTag2.tagId)
      then("the R2 contributor tag is added at the bottom of the contributor tag list")
      contributor should be(true)
    }
  }

  feature("R2 has multiple publication tags") {
    scenario("Flexible-content has no publication tag") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, publicationTag2, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val mainTags = res.tags.filter(_.tagId == publicationTag2.tagId).nonEmpty
      val publication = (res.publication.tagId == publicationTag.tagId)
      then("the extra publication tag is included in the main tags")
      mainTags should be(true)
      and("the first R2 publication shared tag is the publication tag")
      publication should be(true)
    }

    scenario("One is shared with flexible content") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, publicationTag2, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val mainTags = res.tags.filter(_.tagId == publicationTag2.tagId).nonEmpty
      val publication = (res.publication.tagId == publicationTag.tagId)
      then("the extras publication tags are included in the main tags")
      mainTags should be(true)
      and("the shared tag is the publicatio tag")
      publication should be(true)
    }
  }

  feature("Lead tag discrepancy") {
    scenario("Missing Lead tag in R2") {
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(noLeadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val leadtag = (res.tags.length == 1) && res.tags.head.isLead
      then("the main tags contains the lead tag")
      leadtag should be(true)
    }

    scenario("Missing Lead tag in flexible-content") {
      val flexiTags = FlexiTags(List(noLeadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val leadtag = (res.tags.length == 1) && res.tags.head.isLead
      then("the main tags contains the lead tag")
      leadtag should be(true)
    }
  }

  feature("Tag renaming/migration") {
    scenario("The tag has been renamed but keeps the same ID") {
      val flexiTags = FlexiTags(List(mainTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTagRenamed, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val renamed = (res.tags.length == 1) && (res.tags.head.internalName == mainTagRenamed.internalName)
      then("the tag has the correct name")
      renamed should be(true)
    }

    scenario("Only the tag ID has changed after section migration") {
      val flexiTags = FlexiTags(List(oldTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(newTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)

      val jsonMapping = TagDiffer.correctFlexiRepresentation(List[Content](content)).head
      val res = jsonMapping.validate[TagMapping].get
      val migrated = (res.tags.length == 1) && (res.tags.head.tagId == newTag.tagId)
      then("the tag has the correct ID")
      migrated should be(true)
    }
  }
}