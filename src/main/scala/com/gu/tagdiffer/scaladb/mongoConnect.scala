package com.gu.tagdiffer.scaladb

import com.gu.management.Loggable
import com.gu.tagdiffer.Config
import com.gu.tagdiffer.index.model.ContentCategory.Category
import com.gu.tagdiffer.index.model.{ContentCategory, FlexiTags, Content}
import play.api.libs.iteratee.{Enumerator, Iteratee}
import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument
import reactivemongo.core.nodeset.Authenticate
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

object mongoConnect extends Loggable {
  // Get an instance of the driver
  val driver = new MongoDriver
  val uri = MongoConnection.parseURI(Config.mongoURI)
  val connection = uri.map(driver.connection)

  // Get an instance of db
  val db = connection.map(_("flexible-content"))

  // We get a BSONCollection by default
  // adding tipes solves an issue with scala plugin (https://groups.google.com/forum/#!topic/reactivemongo/7b1waOYgTMA)
  val live: Try[BSONCollection] = db.map(_("liveContent"))
  val draft: Try[BSONCollection] = db.map(_("draftContent"))

  // prepare query and filter
  val query = BSONDocument()
  val filter = BSONDocument("identifiers" -> 1, "taxonomy" -> 1, "type" -> 1, "contentChangeDetails" -> 1)

  def liveContent(): Future[List[Content]] = {
    val enumeratorOfFlexiContent = live.get.find(query, filter).options(QueryOpts(batchSizeN = 100)).cursor[FlexiContent].enumerate()

    getContent(enumeratorOfFlexiContent, ContentCategory.Live)
  }

  def draftContent(): Future[List[Content]] = {
    val enumeratorOfFlexiContent = draft.get.find(query, filter).options(QueryOpts(batchSizeN = 100)).cursor[FlexiContent].enumerate()

    getContent(enumeratorOfFlexiContent, ContentCategory.Draft)
  }

 private def getContent(enumerator: Enumerator[FlexiContent], category: Category): Future[List[Content]] = {
   val processDocuments: Iteratee[FlexiContent, List[Content]] = Iteratee.fold(List.empty[Content]) { (acc, content) =>
     val c = if (content.pageId.isDefined)
       Content.lookupR2(content.pageId.get, content.contentId, content.contentType, content.created, content.lastModified,
         category, FlexiTags(content.mainTags.getOrElse(List()), content.contributorTags.getOrElse(List()),
         content.publicationTags.getOrElse(List()), content.bookTags.getOrElse(List()),
         content.sectionTags.getOrElse(List())))
     else None
     c ++: acc
   }

   enumerator.run(processDocuments)
 }
}
