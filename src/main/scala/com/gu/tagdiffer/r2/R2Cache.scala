package com.gu.tagdiffer.r2

import com.gu.tagdiffer.index.model.{Tag, Tagging, TagAudit}
import org.joda.time.DateTime

case class R2Tag()

case class R2Cache(contentPageId: Map[Long, Long],
                   leadTags: Map[Long, List[Long]],
                   contentToTag: Map[Long, List[ContentToTag]],
                   lastModified: Map[Long, DateTime])

case class R2(tagMapper: Map[Long, R2DbTag], liveCache: R2Cache, draftCache: R2Cache, deletedTags: Set[Long], mergeTagOperations: List[(Long, Long)]) {

  lazy val tagIdToTag = tagMapper.map { case (id, r2Tag) =>
    id -> Tag(id, r2Tag.tagType, r2Tag.internalName, r2Tag.externalName, r2Tag.section, existInR2 = Some(true))
  }

  private def lookupR2Tags(pageId: Long, cache: R2Cache): Option[List[Tagging]] = {
    val contentId = cache.contentPageId.get(pageId)
    contentId.map { cId =>
      val tagIds = cache.contentToTag.getOrElse(cId, Nil)
      val leadTagIds = cache.leadTags.getOrElse(cId, Nil)

      tagIds.map { t =>
        val tag = tagIdToTag(t.tagId)
        Tagging(tag, leadTagIds.contains(t.tagId))
      }
    }
  }

  def lookupR2DraftTags(id: String): Option[List[Tagging]] = {
    lookupR2Tags(id.toLong, draftCache)
  }

  def lookupR2LiveTags(id: String): Option[List[Tagging]] = {
    lookupR2Tags(id.toLong, liveCache)
  }

  def isR2Tag(tagID: Long): Boolean = tagMapper.contains(tagID)

  private def lookupR2LastModified(pageId: Long, cache: R2Cache): DateTime = {
    val contentId = cache.contentPageId.get(pageId)

    val timestamp = contentId.map(c => cache.lastModified.get(c).get).get

    new DateTime(timestamp)
  }

  def lookupR2DraftLastModified(id: String): DateTime = {
    lookupR2LastModified(id.toLong, draftCache)
  }

  def lookupR2LiveLastModified(id: String): DateTime = {
    lookupR2LastModified(id.toLong, liveCache)
  }
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
      System.err.println(s"Cached draft content tag map (${draftctt.size})")
      val deletedTags = retrieveTagAuditDeletedTagsFromR2()
      val mergedTags = retrieveTagAuditMergedTagsFromR2()
      System.err.println(s"Retrieved tag audit from r2, ${deletedTags.size} tags to delete, ${mergedTags.length} merge operations")
      val liveCache = R2Cache(livecpi.map(_.pageAndContentId).toMap,
                              liveLead,
                              livectt,
                              livecpi.map(ci => (ci.pageAndContentId._2, ci.lastModified)).toMap)
      val draftCache = R2Cache(draftcpi.map(_.pageAndContentId).toMap,
                               draftLead,
                               draftctt,
                               draftcpi.map(ci => (ci.pageAndContentId._2, ci.lastModified)).toMap)
      internalCache = Some(R2(tags, liveCache, draftCache, deletedTags, mergedTags))
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

  private def retrieveLivePageAndContentIdFromR2(): List[ContentInfo] = {
    val provider = new LivePageAndContentIdDataProvider(database)
    provider.getPageContentIdAndLastModified()
  }

  private def retrieveDraftPageAndContentIdFromR2(): List[ContentInfo] = {
    val provider = new DraftPageAndContentIdDataProvider(database)
    provider.getPageContentIdAndLastModified()
  }

  private def retrieveTagToLiveContentFromR2(): Map[Long, List[ContentToTag]] = {
    val provider = new LiveContentToTagDataProvider(database)
    provider.getContentToTag()
  }

  private def retrieveTagToDraftContentFromR2(): Map[Long, List[ContentToTag]] = {
    val provider = new DraftContentToTagDataProvider(database)
    provider.getContentToTag()
  }

    private def retrieveTagAuditDeletedTagsFromR2(): Set[Long] = {
      val provider = new DeletedTagDataProvider(database)
      provider.getDeletedTags()
    }

    private def retrieveTagAuditMergedTagsFromR2(): List[(Long, Long)] = {
      val provider = new MergedTagDataProvider(database)
      provider.getMergedTags()
    }
}
