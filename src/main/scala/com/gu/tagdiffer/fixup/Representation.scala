package com.gu.tagdiffer.fixup

import com.gu.tagdiffer.index.model._
import com.gu.tagdiffer.index.model.`package`._
import play.api.libs.json._

object Representation {
  def correctFlexiRepresentation(discrepancy: Map[ContentId, List[(Set[Tagging], Set[Tagging], ContentId)]],
                                 contentList: List[Content],
                                 tagMigrationCache: Map[Long, Tagging]): Map[ContentId, FlexiTags] = discrepancy.mapValues{ c =>
    val discrepancy = c.head
    val content = contentList.find(_.contentId == discrepancy._3)

    val r2DiffTags = discrepancy._1
    val flexiDiffTags = discrepancy._2
    val originalTagsInR2 = content.map(_.r2Tags).get
    val originalTagsInFlexi = content.map(_.flexiTags).get

    val contributors = fixContributorTags(r2DiffTags, flexiDiffTags, originalTagsInR2, originalTagsInFlexi)
    val publication = fixPublicationTags(r2DiffTags, flexiDiffTags, originalTagsInR2, originalTagsInFlexi)
    val newspaper = fixNewspaperTags(r2DiffTags, flexiDiffTags, originalTagsInR2, originalTagsInFlexi)
    val mainTags = fixMainTags(r2DiffTags, flexiDiffTags, originalTagsInR2, originalTagsInFlexi, tagMigrationCache)
    // Add extraPublicationTag to mainTags
    val tags = mainTags ++ publication._2

    FlexiTags(tags.distinct, contributors, publication._1.toList, newspaper._1.toList, newspaper._2.toList)
  }

  private def fixContributorTags(r2Diff:Set[Tagging],
                                 flexiDiff:Set[Tagging],
                                 r2Original: R2Tags,
                                 flexiOriginal: FlexiTags): List[Tagging] = {
    val r2Contributors = r2Diff.filter(_.tagType == TagType.Contributor)
    val flexiContributors = flexiDiff.filter(t => (t.tagType == TagType.Contributor) && (t.tag.existInR2.getOrElse(false)))
    val sharedContributorTags = r2Original.contributors intersect flexiOriginal.contributors

    val fixedContributorTags = if (r2Contributors.nonEmpty || flexiContributors.nonEmpty) {
      sharedContributorTags ++ flexiContributors ++ r2Contributors
    } else {
      sharedContributorTags
    }

    fixedContributorTags
  }

  private def fixPublicationTags(r2Diff:Set[Tagging],
                                 flexiDiff:Set[Tagging],
                                 r2Original: R2Tags,
                                 flexiOriginal: FlexiTags): (Option[Tagging], Set[Tagging]) = {
    val r2Publication = r2Diff.filter(_.tagType == TagType.Publication)
    val flexiPublication = flexiDiff.filter(t => (t.tagType == TagType.Publication) && (t.tag.existInR2.getOrElse(false)))
    val sharedPublicationTag = r2Original.publications intersect flexiOriginal.publications

    // R2 can have multiple publication tags. Choose the one in common if so
    val fixedPublicationTag =  if (sharedPublicationTag.nonEmpty){
      sharedPublicationTag.headOption
    } else if (r2Publication.nonEmpty) {
      r2Publication.headOption
    } else if (flexiPublication.nonEmpty) {
      flexiPublication.headOption
    } else {
      None
    }

    val extraPublicationTags = (r2Publication - fixedPublicationTag.getOrElse(null)) ++
      (sharedPublicationTag.toSet - fixedPublicationTag.getOrElse(null)) // only r2 can have multiple pub tags

    (fixedPublicationTag, extraPublicationTags)
  }

  private def fixNewspaperTags(r2Diff:Set[Tagging],
                               flexiDiff:Set[Tagging],
                               r2Original: R2Tags,
                               flexiOriginal: FlexiTags): (Option[Tagging], Option[Tagging]) = {
    val r2Book = r2Diff.filter(_.tagType == TagType.Book)
    val r2BookSection = r2Diff.filter(_.tagType == TagType.BookSection)
    val flexiBook = flexiDiff.filter(_.tagType == TagType.Book)
    val flexiBookSection = flexiDiff.filter(_.tagType == TagType.BookSection)
    val sharedBookTag = r2Original.book intersect flexiOriginal.book
    val sharedBookSectionTag = r2Original.bookSection intersect flexiOriginal.bookSection

    val fixedBook = if (sharedBookTag.nonEmpty) {
      sharedBookTag.headOption
    } else if (r2Book.nonEmpty) {
      r2Book.headOption
    } else if (flexiBook.nonEmpty) {
      flexiBook.headOption
    } else {
      None
    }

    val fixedBookSection = if (sharedBookSectionTag.nonEmpty) {
      sharedBookSectionTag.headOption
    } else if (r2BookSection.nonEmpty) {
      r2BookSection.headOption
    } else if (flexiBookSection.nonEmpty) {
      flexiBookSection.headOption
    } else {
      None
    }

    (fixedBook, fixedBookSection)
  }

  private def fixMainTags(r2Diff:Set[Tagging],
                          flexiDiff:Set[Tagging],
                          r2Original: R2Tags,
                          flexiOriginal: FlexiTags,
                          tagMigrationCache: Map[Long, Tagging]): List[Tagging] = {
    // Tags
    val r2Tags = r2Diff.filterNot(t => (t.tagType == TagType.Book) || (t.tagType == TagType.BookSection) ||
      (t.tagType == TagType.Contributor) || (t.tagType == TagType.Publication))
    val flexiTags = flexiDiff.filterNot(t => (t.tagType == TagType.Book) || (t.tagType == TagType.BookSection) ||
      (t.tagType == TagType.Contributor) || (t.tagType == TagType.Publication))
    val sharedTags = r2Original.other intersect flexiOriginal.other

    val updatedFlexiTags = flexiTags.map( t => t.tagId match {
      case id if (tagMigrationCache.contains(id)) => {
        val newTag = tagMigrationCache.get(id)
        Tagging(newTag.get.tag, t.isLead)
      }
      case _ => t
    }).filter(_.tag.existInR2.getOrElse(false))

    // fix lead tag discrepancy among tags with migrated section
    val tagsOnlyInFlexi = updatedFlexiTags.groupBy(_.tagId)
    val updatedR2Tags = r2Tags.map { r2 =>
      r2.tagId match {
        case id if (tagsOnlyInFlexi.contains(id)) => {
          val isFlexiTagLead = tagsOnlyInFlexi.get(id).map(_.head.isLead)
          Tagging(r2.tag, r2.isLead || isFlexiTagLead.getOrElse(false))
        }
        case _ => r2
      }
    }

    // R2 has now the correct representation of migrated tags and lead tags discrepancy
    val ft = updatedFlexiTags.filterNot(t => updatedR2Tags.exists(_.tagId == t.tagId))

    val orderedUpdatedTags = for {
      t <- r2Original.other

      ut = if (sharedTags.contains(t)) {
        Some(t)
      } else if (updatedR2Tags.map(_.tagId).contains(t.tagId)) {
        updatedR2Tags.find(_.tagId == t.tagId)
      } else {
        None
      }
    } yield ut

    orderedUpdatedTags.filter(_.isDefined).map(_.get) ++ updatedR2Tags ++ ft
  }

  implicit val SectionWrites = Json.writes[Section]

  implicit val TaggingWrites = new Writes[Tagging] {
    override def writes(o: Tagging): JsValue = {
      Json.obj(
        "tag" -> Json.obj(
          "id" -> o.tagId,
          "type" -> o.tagType.toString,
          "internalName" -> o.internalName,
          "externalName" -> o.externalName,
          "section" -> o.section
        )
      )
    }
  }

  val TaggingWritesWithLead = new Writes[Tagging]{
    override def writes(o: Tagging): JsValue =
      TaggingWrites.writes(o) match {
        case fields:JsObject => fields ++ Json.obj("isLead" -> o.isLead)
        case _ => throw new IllegalArgumentException("TaggingWrites didn't return JsObject")
      }
  }

  def jsonTagMapper (contentList: List[Content], discrepancyFix: Map[ContentId, FlexiTags]): List[JsObject] = contentList.filter ( c =>
    discrepancyFix.contains(c.contentId)).map { content =>
    val alltags = discrepancyFix.get(content.contentId).get

    Json.obj(
      "pageId" -> content.pageid,
      "contentId" -> content.contentId,
      "lastModifiedFlexi" -> content.lastModifiedFlexi,
      "lastModifiedR2" -> content.lastModifiedR2,
      "taxonomy" -> Json.obj(
        "tags" -> alltags.other.map(Json.toJson(_)(TaggingWritesWithLead)),
        "contributors" -> alltags.contributors,
        "publication" -> alltags.publications.headOption,
        "newspaper" -> Json.obj(
          "book" -> alltags.book.headOption,
          "bookSection" -> alltags.bookSection.headOption
        )
      )
    )
  }

}
