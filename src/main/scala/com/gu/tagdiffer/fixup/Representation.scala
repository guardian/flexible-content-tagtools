package com.gu.tagdiffer.fixup

import com.gu.tagdiffer.index.model._
import com.gu.tagdiffer.index.model.`package`._
import play.api.libs.json._

object Representation {
  def correctFlexiRepresentation(flexiTags: FlexiTags,
                                 r2Tags: R2Tags, 
                                 tagMigrationCache: Map[Long, Tagging]): FlexiTags = {

    val (onlyR2, onlyFlex) = r2Tags diff flexiTags
    
    if (r2Tags.allTags.size == flexiTags.allTags.size && onlyFlex.isEmpty && onlyR2.isEmpty) flexiTags
    else {
      val contributors = fixContributorTags(onlyR2, onlyFlex, r2Tags, flexiTags)
      val publication = fixPublicationTags(onlyR2, onlyFlex, r2Tags, flexiTags)
      val newspaper = fixNewspaperTags(onlyR2, onlyFlex, r2Tags, flexiTags)
      val mainTags = fixMainTags(onlyR2, onlyFlex, r2Tags, flexiTags, tagMigrationCache)
      // Add extraPublicationTag to mainTags
      val tags = mainTags ++ publication._2

      FlexiTags(tags.distinct, contributors, publication._1.toList, newspaper._1.toList, newspaper._2.toList, newspaper._3.toList)
    }
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
    val flexiPublication = flexiDiff.filter(t => (t.tagType == TagType.Publication) && t.tag.existInR2.getOrElse(false))
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

    val sharedNewspaperPublicationTag = (r2Original.publications intersect flexiOriginal.newspaperPublication).headOption.orNull
    val extraPublicationTags = r2Publication - fixedPublicationTag.orNull - sharedNewspaperPublicationTag

    (fixedPublicationTag, extraPublicationTags)
  }

  private def fixNewspaperTags(r2Diff:Set[Tagging],
                               flexiDiff:Set[Tagging],
                               r2Original: R2Tags,
                               flexiOriginal: FlexiTags): (Option[Tagging], Option[Tagging], Option[Tagging]) = {
    val r2Book = r2Diff.filter(_.tagType == TagType.Book)
    val r2BookSection = r2Diff.filter(_.tagType == TagType.BookSection)
    val flexiBook = flexiDiff.filter(_.tagType == TagType.Book)
    val flexiBookSection = flexiDiff.filter(_.tagType == TagType.BookSection)
    val sharedBookTag = r2Original.book intersect flexiOriginal.book
    val sharedBookSectionTag = r2Original.bookSection intersect flexiOriginal.bookSection

    val sharedNewspaperPublicationTag = r2Original.publications intersect flexiOriginal.newspaperPublication

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

    val fixedNewspaperPublication = if (sharedNewspaperPublicationTag.nonEmpty) {
      sharedNewspaperPublicationTag.headOption
    } else if (flexiOriginal.newspaperPublication.nonEmpty) {
      flexiOriginal.newspaperPublication.headOption
    } else None

    (fixedBook, fixedBookSection, fixedNewspaperPublication)
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

  def jsonTagMapper (content: Content, tags: FlexiTags): JsObject = {
    Json.obj(
      "pageId" -> content.pageid,
      "contentId" -> content.contentId,
      "lastModifiedFlexi" -> content.lastModifiedFlexi,
      "lastModifiedR2" -> content.lastModifiedR2,
      "taxonomy" -> Json.obj(
        "tags" -> tags.other.map(Json.toJson(_)(TaggingWritesWithLead)),
        "contributors" -> tags.contributors,
        "publication" -> tags.publications.headOption,
        "newspaper" -> Json.obj(
          "book" -> tags.book.headOption,
          "bookSection" -> tags.bookSection.headOption,
          "publicaation" -> tags.newspaperPublication.headOption
        )
      )
    )
  }

}
