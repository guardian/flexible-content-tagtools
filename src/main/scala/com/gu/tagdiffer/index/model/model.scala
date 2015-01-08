package com.gu.tagdiffer.index.model

import com.gu.tagdiffer.{TagDiffer, R2}
import com.gu.tagdiffer.TagDiffer.orderAndNumberDiff
import org.joda.time.DateTime

object ContentCategory extends Enumeration {
  type Category = Value
  val Live, Draft = Value
}

case class Content(pageid: String,
                   contentId: ContentId,
                   contentType: String,
                   created: DateTime,
                   lastModifiedR2: DateTime,
                   lastModifiedFlexi: DateTime,
                   flexiTags: FlexiTags,
                   r2Tags: R2Tags) {


  // Calculate the difference betweeen R2 and Flexible-Content tags
  private def diffTags(flexiTags: List[Tagging], r2Tags: List[Tagging]): orderAndNumberDiff = {
    // Diff in order
    val isDifferent = flexiTags != r2Tags

    val flexiSet = flexiTags.toSet
    val r2Set = r2Tags.toSet

    val isOrderDifferent = isDifferent && (flexiSet == r2Set)

    val inFlexButNotR2 = flexiSet diff r2Set
    val inR2ButNotFlex = r2Set diff flexiSet

    val differenceCount = inFlexButNotR2.size + inR2ButNotFlex.size

    (isOrderDifferent, differenceCount)
  }

  def diffTags(selector: Tags => List[Tagging]): orderAndNumberDiff = diffTags(selector(flexiTags), selector(r2Tags))

  private def isDifferent(diffTuple:orderAndNumberDiff) = diffTuple._1 || diffTuple._2 > 0
  def isDifferent(selector: Tags => List[Tagging]): Boolean = isDifferent(diffTags(selector))
}

object Content {
  import TagType._
  def isPublishingTag(tag: Tag): Boolean = Set(Publication, Book, BookSection).contains(tag.tagType)
  def isPublishingTag(tagging: Tagging): Boolean = isPublishingTag(tagging.tag)

  def lookupR2(pageId: String, contentId: ContentId, contentType: String, created: DateTime,
               lastModifiedFlexi: DateTime, contentCategory: ContentCategory.Category, flexiTags: FlexiTags): Option[Content] = {
    val r2Tags =
      if (contentCategory == ContentCategory.Live) R2.cache.lookupR2LiveTags(pageId).map(R2Tags)
      else R2.cache.lookupR2DraftTags(pageId).map(R2Tags)

    val lastModifiedR2 =
      if (contentCategory == ContentCategory.Live) R2.cache.lookupR2LiveLastModified(pageId)
      else R2.cache.lookupR2DraftLastModified(pageId)

    r2Tags.map(Content(pageId, contentId, contentType, created, lastModifiedFlexi, lastModifiedR2, flexiTags, _))
  }
}


trait Tags {
  def allTags: List[Tagging]
  def other: List[Tagging]
  def contributors: List[Tagging]
  def publications: List[Tagging]
  def book: List[Tagging]
  def bookSection: List[Tagging]
}

case class R2Tags(allTags: List[Tagging]) extends Tags {
  lazy val other: List[Tagging] = allTags.filterNot(t => (contributors ::: publications ::: book ::: bookSection).contains(t))
  lazy val contributors: List[Tagging] = allTags.filter(_.tagType == TagType.Contributor)
  lazy val publications: List[Tagging] = allTags.filter(_.tagType == TagType.Publication)
  lazy val book: List[Tagging] = allTags.filter(_.tagType == TagType.Book)
  lazy val bookSection: List[Tagging] = allTags.filter(_.tagType == TagType.BookSection)
}

case class FlexiTags(other: List[Tagging], contributors: List[Tagging], publications: List[Tagging], book: List[Tagging], bookSection: List[Tagging]) extends Tags {
  lazy val allTags = other ::: contributors ::: publications ::: book ::: bookSection
}
