package com.gu.tagdiffer.flexible

import com.gu.management.Loggable
import com.gu.tagdiffer.Config
import com.gu.tagdiffer.index.model.ContentCategory.Category
import com.gu.tagdiffer.index.model.{ContentCategory, FlexiTags, Content}
import play.api.libs.iteratee.{Enumerator, Iteratee}
import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.Try

object mongoConnect extends Loggable {
  // Get an instance of the driver
  val driver = new MongoDriver
  val uri = MongoConnection.parseURI(Config.mongoURI)
//  val connectionOptions = MongoConnectionOptions(readPreference=secondaryPreferred)
  val connection = uri.map(uri => driver.connection(uri))


  val strategy =
    FailoverStrategy(
      initialDelay = 10 seconds,
      retries = 20,
      delayFactor =
        attemptNumber => 1 + attemptNumber * 0.5
    )

  // Get an instance of db
  val db = connection.map(_("flexible_content", strategy))

  // We get a BSONCollection by default
  // adding types solves an issue with scala plugin (https://groups.google.com/forum/#!topic/reactivemongo/7b1waOYgTMA)
  val live: Try[BSONCollection] = db.map(_("liveContent"))
  val draft: Try[BSONCollection] = db.map(_("draftContent"))

  // prepare query and filter
  val query = BSONDocument()
  val filter = BSONDocument("identifiers" -> 1, "taxonomy" -> 1, "type" -> 1, "contentChangeDetails" -> 1)

  def liveContent(): Future[List[Content]] = {

    val enumeratorOfFlexContent = live.get.find(query, filter).options(QueryOpts(batchSizeN = 100)).cursor[FlexContent].enumerate()

    getContent(enumeratorOfFlexContent, ContentCategory.Live)
  }

  def draftContent(): Future[List[Content]] = {
    val enumeratorOfFlexContent = draft.get.find(query, filter).options(QueryOpts(batchSizeN = 100)).cursor[FlexContent].enumerate()

    getContent(enumeratorOfFlexContent, ContentCategory.Draft)
  }

 private def getContent(enumerator: Enumerator[FlexContent], category: Category): Future[List[Content]] = {
   val processDocuments: Iteratee[FlexContent, List[Content]] = Iteratee.fold(List.empty[Content]) { (acc, content) =>
     val c =  try{
         val flexiTags = FlexiTags(content.mainTags.getOrElse(List()), content.contributorTags.getOrElse(List()),
           content.publicationTags.getOrElse(List()), content.bookTags.getOrElse(List()),
           content.sectionTags.getOrElse(List()), content.newspaperPublicationTags.getOrElse(List()))
         Content.lookupR2(content.pageId.get, content.contentId, content.contentType, content.created, content.lastModified, category, flexiTags)
       } catch {
         case NonFatal(e) => None
       }

     c ++: acc
   }

   enumerator.run(processDocuments)
 }
}
