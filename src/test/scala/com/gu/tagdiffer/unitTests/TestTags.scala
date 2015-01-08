package com.gu.tagdiffer.unitTests

import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model.{Tag, Tagging, Section}
import org.joda.time.DateTime

object TestTags {
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
}
