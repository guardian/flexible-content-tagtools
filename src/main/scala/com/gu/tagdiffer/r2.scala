package com.gu.tagdiffer

import com.gu.tagdiffer.component.DatabaseComponent
import com.gu.tagdiffer.database._
import com.gu.tagdiffer.index.model.Tag

case class R2Tag()

case class R2Cache(contentPageId: Map[Long, Long],
                   leadTags: Map[Long, List[Long]],
                   contentToTag: Map[Long, List[ContentToTag]])

case class R2(tagMapper: Map[Long, R2DbTag], liveCache: R2Cache, draftCache: R2Cache) {

  private def lookupR2Tags(pageId: Long, cache: R2Cache): Option[List[Tag]] = {
    val contentId = cache.contentPageId.get(pageId)
    contentId.map { cId =>
      val tagIds = cache.contentToTag.getOrElse(cId, Nil)
      val leadTagIds = cache.leadTags.getOrElse(cId, Nil)

      tagIds.map { t =>
        val r2Tag = tagMapper(t.tagId)
        Tag(t.tagId, r2Tag.name, leadTagIds.contains(t.tagId), r2Tag.tagType, true)
      }
    }
  }

  def lookupR2DraftTags(id: String): Option[List[Tag]] = {
    lookupR2Tags(id.toLong, draftCache)
  }

  def lookupR2LiveTags(id: String): Option[List[Tag]] = {
    lookupR2Tags(id.toLong, liveCache)
  }

  def isR2Tag(tagID: Long): Boolean = tagMapper.contains(tagID)
}
  object R2 extends DatabaseComponent {

    var internalCache: Option[R2] = _

    def init() = {
      System.err.println("Caching R2")
      val tags = retrieveTagTableFromR2()
      System.err.println(s"Cached tag table (${tags.size})")
      val livecpi = retrieveLivePageAndContentIdFromR2()
      System.err.println(s"Cached live page->content map (${livecpi.size})")
      val draftcpi = retrieveDraftPageAndContentIdFromR2()
      System.err.println(s"Cached draft page->content map (${draftcpi.size})")
      val liveLead = retrieveLiveLeadTagsFromR2()
      System.err.println(s"Cached live lead tag map (${liveLead.size})")
      val draftLead = retrieveDraftLeadTagsFromR2()
      System.err.println(s"Cached draft lead tag map (${draftLead.size})")
      val livectt = retrieveTagToLiveContentFromR2()
      System.err.println(s"Cached live content tag map (${livectt.size})")
      val draftctt = retrieveTagToDraftContentFromR2()
      System.err.println(s"Cached live content tag map (${draftctt.size})")
      val liveCache = R2Cache(livecpi, liveLead, livectt)
      val draftCache = R2Cache(draftcpi, draftLead, draftctt)
      internalCache = Some(R2(tags, liveCache, draftCache))
    }

  def cache: R2 = internalCache.getOrElse(throw new IllegalStateException("R2 cache not initialised yet"))

  private def retrieveTagTableFromR2(): Map[Long, R2DbTag] = {
    val provider = new TagTableDataProvider(database)
    provider.getTagTable()
  }

  private def retrieveLiveLeadTagsFromR2(): Map[Long, List[Long]] = {
    val provider = new LiveLeadTagDataProvider(database)
    provider.getLeadTag()
  }

  private def retrieveDraftLeadTagsFromR2(): Map[Long, List[Long]] = {
    val provider = new DraftLeadTagDataProvider(database)
    provider.getLeadTag()
  }

  private def retrieveLivePageAndContentIdFromR2(): Map[Long, Long] = {
    val provider = new LivePageAndContentIdDataProvider(database)
    provider.getPageAndContentId()
  }

  private def retrieveDraftPageAndContentIdFromR2(): Map[Long, Long] = {
    val provider = new DraftPageAndContentIdDataProvider(database)
    provider.getPageAndContentId()
  }

  private def retrieveTagToLiveContentFromR2(): Map[Long, List[ContentToTag]] = {
    val provider = new LiveContentToTagDataProvider(database)
    provider.getContentToTag()
  }

  private def retrieveTagToDraftContentFromR2(): Map[Long, List[ContentToTag]] = {
    val provider = new DraftContentToTagDataProvider(database)
    provider.getContentToTag()
  }
}
