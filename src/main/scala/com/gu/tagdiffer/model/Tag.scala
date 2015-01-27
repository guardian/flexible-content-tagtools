package com.gu.tagdiffer.index.model

import com.gu.tagdiffer.index.model.TagType.TagType
import com.gu.tagdiffer.r2.R2

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

case class Section(id: Long, name: String, pathPrefix: Option[String], slug: String) {
  override def toString: String = s"$id name=[$name] pathPrefix=[$pathPrefix] slug=[$slug]"
}

case class Tagging(tag: Tag, isLead: Boolean) {
  def tagType = tag.tagType
  def tagId = tag.tagId
  def internalName = tag.internalName
  def externalName = tag.externalName
  def section = tag.section

  override def toString: String =
    s"${tag.tagId} [${tag.internalName}] [${tag.externalName}] "+
      s"${if (isLead) " LEAD" else ""} ${tag.tagType}${if (!tag.existInR2.getOrElse(false)) " NOR2" else ""}"+
      s" ${tag.section.toString}"

  def setIdToZero = Tagging(tag.copy(tagId=0), isLead)
}

case class Tag(tagId: Long,
               tagType: TagType,
               internalName: String,
               externalName: String,
               section: Section,
               existInR2: Option[Boolean])

object Tag {
  def createFromFlex(tagId: Long,
                     tagType: TagType,
                     internalName: String,
                     externalName: String,
                     section: Section ) = {
    val exist = R2.cache.isR2Tag(tagId)

    Tag(tagId, tagType, internalName, externalName, section, Some(exist))
  }
}
