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

    // Contributors
    val r2Contributors = discrepancy._1.filter(_.tagType == TagType.Contributor)
    val flexiContributors = discrepancy._2.filter(t => (t.tagType == TagType.Contributor) && (t.tag.existInR2.getOrElse(false)))
    val sharedContributorTags = content.map(c => c.r2Tags.contributors intersect c.flexiTags.contributors)

    val contributors = if (r2Contributors.nonEmpty || flexiContributors.nonEmpty) {
      sharedContributorTags.getOrElse(List.empty[Tagging]) ++ flexiContributors ++ r2Contributors
    } else {
      sharedContributorTags.getOrElse(List.empty[Tagging])
    }
    // Publication
    val r2Publication = discrepancy._1.filter(_.tag.tagType == TagType.Publication)
    val flexiPublication = discrepancy._2.filter(t => (t.tagType == TagType.Publication) && (t.tag.existInR2.getOrElse(false)))
    val sharedPublicationTag = content.map(c => c.r2Tags.publications.toSet intersect c.flexiTags.publications.toSet).getOrElse(Set.empty[Tagging])
    // R2 can have multiple publication tags. Choose the one in common if so
    val publication = if (sharedPublicationTag.nonEmpty){
      sharedPublicationTag.headOption
    } else if (r2Publication.nonEmpty) {
      r2Publication.headOption
    } else if (flexiPublication.nonEmpty) {
      flexiPublication.headOption
    } else {
      None
    }

    val extraPublicationTags = (r2Publication - publication.getOrElse(null)) ++
      (sharedPublicationTag - publication.getOrElse(null))// only r2 can have multiple pub tags
    // Newspaper (Book and Book Section)
    val r2Book = discrepancy._1.filter(_.tagType == TagType.Book)
    val r2BookSection = discrepancy._1.filter(_.tagType == TagType.BookSection)
    val flexiBook = discrepancy._2.filter(_.tagType == TagType.Book)
    val flexiBookSection = discrepancy._2.filter(_.tagType == TagType.BookSection)
    val sharedBookTag = content.map(c => c.r2Tags.book intersect c.flexiTags.book).getOrElse(List.empty[Tagging])
    val sharedBookSectionTag = content.map(c => c.r2Tags.bookSection intersect c.flexiTags.bookSection).getOrElse(List.empty[Tagging])
    val book = if (sharedBookTag.nonEmpty) {
      sharedBookTag.headOption
    } else if (r2Book.nonEmpty) {
      r2Book.headOption
    } else if (flexiBook.nonEmpty) {
      flexiBook.headOption
    } else {
      None
    }

    val bookSection = if (sharedBookSectionTag.nonEmpty) {
      sharedBookSectionTag.headOption
    } else if (r2BookSection.nonEmpty) {
      r2BookSection.headOption
    } else if (flexiBookSection.nonEmpty) {
      flexiBookSection.headOption
    } else {
      None
    }
    // Tags
    val r2Tags = discrepancy._1.filterNot(t => (t.tagType == TagType.Book) || (t.tagType == TagType.BookSection) ||
      (t.tagType == TagType.Contributor) || (t.tagType == TagType.Publication))
    val flexiTags = discrepancy._2.filterNot(t => (t.tagType == TagType.Book) || (t.tagType == TagType.BookSection) ||
      (t.tagType == TagType.Contributor) || (t.tagType == TagType.Publication))
    val sharedTags = content.map(c => c.r2Tags.other intersect c.flexiTags.other)

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
    // Preserve the original order (R2 order)
    val r2OriginalTags = content.map(_.r2Tags).get

    val orderedUpdatedTags = for {
      t <- r2OriginalTags.other

      ut = if (sharedTags.getOrElse(List.empty[Tagging]).contains(t)) {
        Some(t)
      } else if (updatedR2Tags.map(_.tagId).contains(t.tagId)) {
        updatedR2Tags.find(_.tagId == t.tagId)
      } else {
        None
      }
    } yield ut

    val tags = orderedUpdatedTags.filter(_.isDefined).map(_.get) ++ updatedR2Tags ++ ft  ++ extraPublicationTags

    FlexiTags(tags.distinct, contributors, publication.toList, book.toList, bookSection.toList)
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
