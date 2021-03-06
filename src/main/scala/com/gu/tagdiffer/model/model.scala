package com.gu.tagdiffer.index.model

import com.gu.tagdiffer.TagDiffer
import com.gu.tagdiffer.TagDiffer.orderAndNumberDiff
import com.gu.tagdiffer.r2.R2
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
  // these are lists even though there's only 1 of each tag in flex to support the R2 model
  def book: List[Tagging]
  def bookSection: List[Tagging]
//  def newspaperPublication: List[Tagging]

  /**
   * Returns a tuple of two sets, the first containing tags that exist in this set but not other and the second
   * containing tags that exist in the other set but not this set
   * @param other
   * @return
   */
  def diff(other: Tags) = {
    val thisTagSet = allTags.toSet
    val otherTagSet = other.allTags.toSet
    (thisTagSet diff otherTagSet, otherTagSet diff thisTagSet)
  }

  def setEquals(other: Tags): Boolean = {
    val (set1, set2) = diff(other)
    set1.isEmpty && set2.isEmpty
  }
}

case class R2Tags(allTags: List[Tagging]) extends Tags {
  lazy val other: List[Tagging] = allTags.filterNot(t => (contributors ::: publications ::: book ::: bookSection).contains(t))
  lazy val contributors: List[Tagging] = allTags.filter(_.tagType == TagType.Contributor)
  lazy val publications: List[Tagging] = allTags.filter(_.tagType == TagType.Publication)
  lazy val book: List[Tagging] = allTags.filter(_.tagType == TagType.Book)
  lazy val bookSection: List[Tagging] = allTags.filter(_.tagType == TagType.BookSection)

  override def toString = allTags.map(_.toString).mkString("\n")
}

case class FlexiTags(other: List[Tagging], contributors: List[Tagging], publications: List[Tagging], book: List[Tagging], bookSection: List[Tagging], newspaperPublication: List[Tagging]) extends Tags {
  def stringWithDiffs(original: FlexiTags): String = {
    val (onlyNew, onlyOriginal) = diff(original)
    val originalSet = original.allTags.toSet
    tagMarkerTuples.map{ case (tag, marker) =>
      val prefix: String =
        if (onlyNew.contains(tag)) "+"
        else if (originalSet.contains(tag)) "="
        else "?"
      s"$prefix[$marker] $tag"
    }.mkString("\n")
  }

  lazy val allTags = other ::: contributors ::: publications ::: book ::: bookSection ::: newspaperPublication

  def tagToStringWithMarker (tags: List[Tagging], marker: String): List[(Tagging,String)] = tags.map( t => (t,marker))
  lazy val tagMarkerTuples =
    tagToStringWithMarker(other, "M") :::
    tagToStringWithMarker(contributors, "C") :::
    tagToStringWithMarker(publications, "P") :::
    tagToStringWithMarker(book, "B") :::
    tagToStringWithMarker(bookSection, "S") :::
    tagToStringWithMarker(newspaperPublication, "NP")

  override def toString = tagMarkerTuples.map{ case (tag, marker) => s"[$marker] $tag" }.mkString("\n")

}
