package com.gu.tagdiffer.index.model

import com.gu.tagdiffer.R2
import com.gu.tagdiffer.index.model.TagType.TagType

object TagType extends Enumeration {
  type TagType = Value
  val Contributor = Value("Contributor")
  val Publication = Value("Publication")
  val Book = Value("Newspaper Book")
  val BookSection = Value("Newspaper Book Section")
  val Other = Value("Other")
  val Keyword = Value("Keyword")
  val Tone = Value("Tone")
  val Series = Value("Series")
  val ContentType = Value("Content Type")
  val Blog = Value("Blog")
}

case class Tag(tagId: Long, internalName: String, isLead: Boolean, tagType: TagType, existInR2: Boolean) {
  override def toString: String = s"$tagId [$internalName]${if (isLead) " LEAD" else ""} $tagType${if (!existInR2) " NOR2" else ""}"
}


object Tag {
   def createFromFlex (tagId: Long, internalName: String, isLead: Boolean, tagType: TagType) = {
    val exist = R2.cache.isR2Tag(tagId)

    Tag(tagId, internalName, isLead, tagType, exist)
  }
}
