package com.gu.tagdiffer.database

import com.gu.tagdiffer.index.model.{Section, TagType}
import com.gu.tagdiffer.index.model.TagType.TagType
import com.gu.tagdiffer.scaladb._
import org.joda.time.DateTime

case class ContentInfo (pageAndContentId: (Long, Long), lastModified: DateTime)
 case class ContentToTag(tagId:Long, order:Long)
 case class R2DbTag(tagType: TagType, internalName: String, externalName: String, slug: String, section: Section)

 class DraftContentToTagDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/draft-content-to-tags-query.sql")

 class LiveContentToTagDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/live-content-to-tags-query.sql")

 class TagTableDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/tags-query.sql")

 class LiveLeadTagDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/live-lead-tag-query.sql")

 class DraftLeadTagDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/draft-lead-tag-query.sql")

 class LivePageAndContentIdDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/live-pageId-contentId-query.sql")

 class DraftPageAndContentIdDataProvider(database: Database)
  extends ContentToTagDataProvider(database, "sql/draft-pageId-contentId-query.sql")

sealed abstract class ContentToTagDataProvider(database: Database, queryFilename: String) {

  protected def contentToTagFromRow(row: Row): (Long, Long, Long) = {
    (row("content_id").long, row("tag_id").long, row("sort").long)
  }

  protected def tagIdToTagNameFromRow(row: Row): (Long, R2DbTag) = {
    val id = row("tag_id").long
    val internal_name = row("tag_internal_name").string
    val external_name = row("tag_external_name").string
    val slug = row("slug").string
    // Section
    val sectionId = row("section_id").long
    val sectionName = row("section_name").string
    val sectionSlug = row("section_slug").string
    val pathPrefix = row("path_prefix").string //Substring

    val section = Section(sectionId, sectionName, parseCmsPath(pathPrefix), sectionSlug)

    val tagType =
     if (row("publication_tag").nullableLong.isDefined) TagType.Publication
     else if (row("contributor_tag").nullableLong.isDefined) TagType.Contributor
     else if (row("newspaper_book_tag").nullableLong.isDefined) TagType.Book
     else if (row("newspaper_book_section_tag").nullableLong.isDefined) TagType.BookSection
     else if (row("keyword_tag").nullableLong.isDefined) TagType.Keyword
     else if (row("series_tag").nullableLong.isDefined) TagType.Series
     else if (row("tone_tag").nullableLong.isDefined) TagType.Tone
     else if (row("blog_tag").nullableLong.isDefined) TagType.Blog
     else if (row("content_type_tag").nullableLong.isDefined) TagType.ContentType
     else TagType.Other

    id -> R2DbTag(tagType, internal_name, external_name, slug, section)
  }

  protected def leadTagFromRow(row: Row): (Long, Long) =
    row("content_id").long -> row("tag_id").long

  protected def pageContentIdLastModifiedFromRow(row: Row): ContentInfo =
    ContentInfo((row("page_id").long -> row("content_id").long), row("last_modified").jodaDateTime)

  def getContentToTag(): Map[Long, List[ContentToTag]] = {
    val query = StoredQuery.fromClasspath(queryFilename)
    val contentToTag = database { con =>
      con.query(query, contentToTagFromRow).
        foldLeft(Map.empty[Long, List[ContentToTag]]){ case (acc, (contentId, tagId, sortOrder)) =>
          acc + (contentId -> (ContentToTag(tagId, sortOrder) :: acc.getOrElse(contentId, Nil)))
        }
    }

    contentToTag.mapValues(tags => tags.sortBy(_.order))
  }

  def getTagTable(): Map[Long, R2DbTag] = {
    val query = StoredQuery.fromClasspath(queryFilename)

    val tags = database { con =>
      con.query(query, tagIdToTagNameFromRow).toMap
    }

    tags
  }

  def getLeadTag(): Map[Long, List[Long]] = {
    val query = StoredQuery.fromClasspath(queryFilename)

    val leadT = database { con =>
      con.query(query, leadTagFromRow).
        foldLeft(Map.empty[Long,List[Long]]) { case (acc, (contentId, tagId)) =>
        acc + (contentId -> (tagId :: acc.getOrElse(contentId, Nil)))
      }
    }

    leadT
  }

  def getPageContentIdAndLastModified(): List[ContentInfo] = {
    val query = StoredQuery.fromClasspath(queryFilename)

    val pageToContentId = database { con =>
      con.query(query, pageContentIdLastModifiedFromRow).toList
    }

    pageToContentId
  }

  private def parseCmsPath(cmsPath: String): Option[String] = {
    val cmsPathPattern = """/Guardian/(.*)""".r
    cmsPath match {
      case cmsPathPattern(path) => Some(path)
      case _ => None
    }
  }
}
