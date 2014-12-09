package com.gu.tagdiffer.index.model

import com.gu.tagdiffer.R2
import com.gu.tagdiffer.TagDiffer.orderAndNumberDiff
import org.joda.time.DateTime

object ContentCategory extends Enumeration {
  type Category = Value
  val Live, Draft = Value
}

case class Content(pageid: String,
                   contentId: String,
                   contentType: String,
                   created: DateTime,
                   lastModifiedR2: DateTime,
                   lastModifiedFlexi: DateTime,
                   flexiTags: FlexiTags,
                   r2Tags: R2Tags) {


  // Calculate the difference betweeen R2 and Flexible-Content tags
  private def diffTags(flexiTags: List[Tag], r2Tags: List[Tag]): orderAndNumberDiff = {
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

  def diffTags(selector: Tags => List[Tag]): orderAndNumberDiff = diffTags(selector(flexiTags), selector(r2Tags))

  private def isDifferent(diffTuple:orderAndNumberDiff) = diffTuple._1 || diffTuple._2 > 0
  def isDifferent(selector: Tags => List[Tag]): Boolean = isDifferent(diffTags(selector))
}

object Content {
  import TagType._
  def isPublishingTag(tag: Tag) = Set(Publication, Book, BookSection).contains(tag.tagType)

  def lookupR2(pageId: String, contentId: String, contentType: String, created: DateTime, lastModifiedFlexi: DateTime, contentCategory: ContentCategory.Category, flexiTags: FlexiTags): Option[Content] = {
    val r2Tags =
      if (contentCategory == ContentCategory.Live) R2.cache.lookupR2LiveTags(pageId).map(R2Tags)
      else R2.cache.lookupR2DraftTags(pageId).map(R2Tags)

    val lastModifiedR2 =  if (contentCategory == ContentCategory.Live) R2.cache.lookupR2LiveLastModified(pageId)
    else R2.cache.lookupR2DraftLastModified(pageId)

    r2Tags.map(Content(pageId, contentId, contentType, created, lastModifiedFlexi, lastModifiedR2, flexiTags, _))
  }
}


trait Tags {
  def allTags: List[Tag]
  def other: List[Tag]
  def contributors: List[Tag]
  def publications: List[Tag]
  def book: List[Tag]
  def bookSection: List[Tag]
}

case class R2Tags(allTags: List[Tag]) extends Tags {
  lazy val other: List[Tag] = allTags.filterNot(t => (contributors ::: publications ::: book ::: bookSection).contains(t))
  lazy val contributors: List[Tag] = allTags.filter(_.tagType == TagType.Contributor)
  lazy val publications: List[Tag] = allTags.filter(_.tagType == TagType.Publication)
  lazy val book: List[Tag] = allTags.filter(_.tagType == TagType.Book)
  lazy val bookSection: List[Tag] = allTags.filter(_.tagType == TagType.BookSection)
}

case class FlexiTags(other: List[Tag], contributors: List[Tag], publications: List[Tag], book: List[Tag], bookSection: List[Tag]) extends Tags {
  lazy val allTags = other ::: contributors ::: publications ::: book ::: bookSection
}
