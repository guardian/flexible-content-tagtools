package com.gu.tagdiffer

import java.io.{File, PrintWriter}
import com.gu.tagdiffer.cache.FileCache
import com.gu.tagdiffer.fixup.Representation
import com.gu.tagdiffer.index.model.ContentCategory._
import com.gu.tagdiffer.index.model.TagType
import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model._
import com.gu.tagdiffer.flexible.mongoConnect
import com.gu.tagdiffer.r2.{R2, DatabaseComponent}
import play.api.libs.json._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal


object TagDiffer extends DatabaseComponent {
  // the path where we write result and cache
  val PREFIX = "target/tag-differ"

  /*
   * (Boolean, Int) :
   * - The Boolean value indicates whether tags of the same content are the same but in a different order
   * - The Int value count the number of different tags for the same content
   */
  type orderAndNumberDiff = (Boolean, Int)

  // We can produce two different result so far: Write to a csv file or print to screen
  trait ComparatorResult

  case class CSVFileResult(fileName:String, header:String, lines:Iterable[String]) extends ComparatorResult
  case class JSONFileResult(fileName: String, lines: Iterable[JsObject]) extends ComparatorResult
  case class ScreenResult(lines:Iterable[String]) extends ComparatorResult

  case class TagCorrected(tags: List[Tagging], contributors: List[Tagging],
                          publication: Option[Tagging], book: Option[Tagging], bookSection: Option[Tagging])

  trait ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]): Iterable[ComparatorResult]
  }

  def comparePermutations(tagDiff: List[(Option[Tagging], Set[Tagging])], tagType: TagType): ComparatorResult = {
    val tagDiffCount = tagDiff.groupBy(tag => tag).mapValues(_.size)
    val header = "Flexible, R2, Count"
    val lines = tagDiffCount.toList.sortBy(_._2).reverse map { case ((flex, r2), count) =>
      s"${flex.map(_.internalName)}, ${r2.map(_.internalName).mkString(";")}, $count"
    }
    CSVFileResult(s"${ContentCategory.toString()}_${tagType.toString}_tag_report.csv", header, lines)
  }

  def compareDeltas(deltas: List[(Set[Tagging],Set[Tagging], String)], name:String): ComparatorResult = {
    val deltaCount = deltas.groupBy(delta => (delta._1, delta._2)).mapValues(v => (v.size, v.head._3)).toList.sortBy(_._2).reverse
    val lines = deltaCount.map { case ((r2, flex), (count, contentID)) =>
      val r2Tags = r2.toList.sortBy(_.internalName)
      val flexTags = flex.toList.sortBy(_.internalName).mkString("\"", "\n", "\"")
      s"$r2Tags,$flexTags,$count, ${Config.composerContentPrefix}$contentID"
    }
    CSVFileResult(s"$name-delta-report.csv", "Only R2, Only Flex, Count, Example Content Id", lines)
  }

  val summaryDiff = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.map { c =>
        val (cat, contents) = c
        val nonPubDiffContent = contents.filter(_.isDifferent(_.allTags.filterNot(Content.isPublishingTag)))
        ScreenResult(List(
          s"Total number of ${cat.toString} contents ${contents.size}",
          s"Number of contents with different tags ${contents.count(_.isDifferent(_.allTags))}",
          s"Number of contents with different tags excluding publishing tags ${nonPubDiffContent.size}"
        ))
      }
    }
  }
  /*
   * For each year create a csv file.
   * The results show how many tags differ for each content and
   * map these numbers to the number of contents that have the same number of diff tags
   */
  val numberDiffByYear = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.flatMap { case (category, content) =>
        val contentCategory = category
        val yearToContent = content.groupBy(_.created.getYear)
        // Generate a separate .csv for each year
        yearToContent.map { ytc =>
          val totalContent = ytc._2.length
          val header = s"total content: $totalContent\nnumber of different tags, content affected"
          val lines = ytc._2.filter {
            _.isDifferent(_.allTags)
          }.groupBy {
            _.diffTags(_.allTags)
          }.toList.sortBy(_._1) map (diff =>
            s"${diff._1}, ${diff._2.length}"
            )
          CSVFileResult(s"${contentCategory.toString}_diffTags_${ytc._1}.csv", header, lines)
        }
      }
    }
  }

  val publicationTagDiffs = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.map { case (category, content) =>
        val pubTagDiff = content.filter(_.isDifferent(_.publications)).map { c =>
          (c.flexiTags.publications.headOption, c.r2Tags.publications.toSet)
        }
        comparePermutations(pubTagDiff, TagType.Publication)
      }
    }
  }

  val bookTagDiffs = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.map { case (category, content) =>
        val bookTagDiff = content.filter(_.isDifferent(_.book)).map { c =>
          (c.flexiTags.book.headOption, c.r2Tags.book.toSet)
        }
        comparePermutations(bookTagDiff, TagType.Book)
      }
    }
  }

  val sectionTagDiffs = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.map { case (category, content) =>
        val sectionTagDiff = content.filter(_.isDifferent(_.bookSection)).map { c =>
          (c.flexiTags.bookSection.headOption, c.r2Tags.bookSection.toSet)
        }
        comparePermutations(sectionTagDiff, TagType.BookSection)
      }
    }
  }

  val flexiNewspaperDuplication = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.map { case (category, content) =>
        val contentWithErrantNewspaperTags = content.filter(_.flexiTags.other.exists(Content.isPublishingTag))
        val discrepancy = contentWithErrantNewspaperTags.count { c =>
          val otherTags = c.flexiTags.other.filter(Content.isPublishingTag).toSet
          val newspaperTags = (c.flexiTags.book :: c.flexiTags.bookSection).toSet

          otherTags == newspaperTags
        }
        val sorted = contentWithErrantNewspaperTags.sortBy(_.created.getMillis)
        ScreenResult(List(
          s"newspaper tags in main tags list for ${category.toString} are: ${contentWithErrantNewspaperTags.size}",
          s"Content date range with different tags: ${sorted.head.created} to ${sorted.last.created}",
          s"Newspaper tags discrepancy: ${discrepancy}"
        ))
      }
    }
  }

  def r2FlexDiffTuples(contentList: List[Content]): List[(Set[Tagging], Set[Tagging], ContentId)] = contentList.map { content =>
    val r2TagSet = content.r2Tags.allTags.toSet
    val flexTagSet = content.flexiTags.allTags.toSet
    val onlyR2 = r2TagSet diff flexTagSet
    val onlyFlex = flexTagSet diff r2TagSet
    (onlyR2, onlyFlex, content.contentId)
  }

  val setOfDeltas = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap.map { case (category, contentList) =>
        val deltas = r2FlexDiffTuples(contentList)
        compareDeltas(deltas, s"all-tags-${category.toString}")
      }
    }
  }

  def computeOldToNewTagIdMap(contentMap: Map[Category, List[Content]]):Map[Long, Long] = {
    val deltas = contentMap.flatMap { case (category, contentList) =>
      r2FlexDiffTuples(contentList).filterNot { case (r2Only, flexOnly, contentId) =>
        r2Only.map(_.setIdToZero) == flexOnly.map(_.setIdToZero)
      }
    }

    deltas.flatMap { e =>
      val r2OnlyKeyword = e._1.filter(tag => tag.tagType != Publication && tag.tagType != Contributor)
      val flexOnlyKeyword = e._2.filter(tag => tag.tagType != Publication && tag.tagType != Contributor)

      val diffIdOnly = for {
        r2t <- r2OnlyKeyword; ft <- flexOnlyKeyword

        if ((r2t.internalName == ft.internalName)
          && (r2t.tagId != ft.tagId))
      } yield {
        ft.tagId -> r2t.tagId
      }

      diffIdOnly.toSet
    }.toMap
  }

  def mapSectionMigrationTags(deltas:List[(Set[Tagging], Set[Tagging])], oldToNewTagIdMap:Map[Long, Long]): Map[Long, Tagging] = {
    val r2Tags = deltas.flatMap(_._1).toSet
    val flexiTags = deltas.flatMap(_._2).toSet

    oldToNewTagIdMap map { case(oldId, newId) =>
      val r2t = r2Tags.find(_.tagId == newId).get
      val ftOpt = flexiTags.find { tag =>
        (tag.tagId == oldId) && (tag.section.id != r2t.section.id)
      }.orElse {
        flexiTags.find(_.tagId == oldId)
      }

      ftOpt.map(_.tagId).get -> r2t
    }
  }

  def compareAndMapDifferentTagIds(deltas:List[(Set[Tagging], Set[Tagging])], oldToNewTagIdMap: Map[Long, Long], name:String): ComparatorResult = {
    mapSectionMigrationTags(deltas, oldToNewTagIdMap)

    val header = "Old Tag Id, Updated Tag Id, Old Internal Name, Updated Internal Name, Old External Name, Updated External Name, " +
      "Old Type, Updated Type, Old Section Id, Updated Section Id, Old Section Name, Updated Section Name"

    val flexiTags = deltas.flatMap(_._2).toSet

    val lines = mapSectionMigrationTags(deltas, oldToNewTagIdMap) flatMap { case(oldId, newTag) =>
      val ftOpt = flexiTags.find { tag =>
        (tag.tagId == oldId) && (tag.section.id != newTag.section.id)
      }.orElse {
        flexiTags.find(_.tagId == oldId)
      }

      ftOpt.map{ ft =>
        s"${ft.tagId}, ${newTag.tagId}, ${ft.internalName}, ${newTag.internalName}, ${newTag.externalName}, ${newTag.externalName}, " +
          s"${ft.tagType}, ${newTag.tagType}, ${ft.section.id}, ${newTag.section.id}, ${ft.section.name}, ${newTag.section.name}"
      }
    }

    CSVFileResult(s"$name.csv", header, lines)
  }

  val setOfDeltasWithoutRemappedTags = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      val deltas = contentMap flatMap { case(category, contentList) =>
        r2FlexDiffTuples(contentList)
      }

      Some(compareAndMapDifferentTagIds(deltas.map(item => (item._1, item._2)).toList, oldToNewTagIdMap, s"updated-tags-map"))
    }
  }

  val setOfDeltasWithoutSectionMigrationTags = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]) = {
      contentMap map { case(category, contentList) =>
        val updatedContentList = contentList map { content =>
          val flexiOtherTagging = content.flexiTags.other
          val r2OtherTagging = content.r2Tags.other

          val newListOfOtherTagging = flexiOtherTagging.foldLeft(List.empty[Tagging]){ (acc, tagging) =>
            val t = if (oldToNewTagIdMap.contains(tagging.tagId)) {
              val newId = oldToNewTagIdMap.get(tagging.tagId).get
              r2OtherTagging.find(_.tagId == newId).getOrElse(tagging)
            } else {
              tagging
            }
            t :: acc
          }

          val updatedFlexiTags = content.flexiTags.copy(other = newListOfOtherTagging)
          content.copy(flexiTags = updatedFlexiTags)
        }
        val deltas = r2FlexDiffTuples(updatedContentList)
        compareDeltas(deltas, s"updated-tags-${category.toString}")
      }
    }
  }

  val correctTagMapping = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]], oldToNewTagIdMap: Map[Long, Long]): Iterable[ComparatorResult] = {
      // Find and filter content with tag discrepancies
      contentMap flatMap { case(category, contentList) =>
        val tuples = r2FlexDiffTuples(contentList).filterNot(c => c._1.isEmpty && c._2.isEmpty)
        val diffTags = tuples.groupBy(_._3)

        /*
        * Find content that only has duplicated newspaper tags in the main tag list
        * This problem comes from inCopy integration and only affect flexible-content
        * (Fixed in the integration but old content is still affected)
        */
        val newspaperTagsDuplication = contentList.filterNot(c => diffTags.contains(c.contentId)).map { content =>
          val duplicatedContent = content.flexiTags.other.filter(t => (t.tagType == TagType.Book) || (t.tagType == BookSection))
          val res = if (!duplicatedContent.isEmpty) {
            Some((Set.empty[Tagging], Set.empty[Tagging], content.contentId))
          } else {
            None
          }
          res
        }.filter(_.isDefined).map(_.get).groupBy(_._3)

        val discrepancy = diffTags ++ newspaperTagsDuplication

        // create mapping of tags with migrated section
        val tagMigrationCache = mapSectionMigrationTags(tuples.map(t => (t._1, t._2)), oldToNewTagIdMap)
        // fix and map
        val discrepancyMap = discrepancy.mapValues(d => (d.head._1, d.head._2)).map{ d =>
          val content = contentList.find(_.contentId == d._1).get
          //ContentID -> (r2Diff, FlexiDiff, originalR2Tags, originalFlexiTags)
          d._1 -> (d._2._1, d._2._2, content.r2Tags, content.flexiTags)
        }


        val discrepancyFix = discrepancyMap.mapValues { case (r2DiffTags, flexiDiffTags, originalTagsInR2, originalTagsInFlexi) =>
          val proposedFlexTags = Representation.correctFlexiRepresentation(
            r2DiffTags, flexiDiffTags, originalTagsInR2, originalTagsInFlexi, tagMigrationCache)
          (proposedFlexTags, originalTagsInR2, originalTagsInFlexi)
        }
        val mapping = Representation.jsonTagMapper(contentList, discrepancyFix.mapValues(_._1))

        val lines = discrepancyFix.map { d =>
          val content = contentList.find(_.contentId == d._1)
          val flexiTags = content.map(_.flexiTags)
          val r2Tags = content.map(_.r2Tags)
          val fixTags = d._2._1

          val flexiTagsString = flexiTags.map(_.toString).getOrElse("No content")
          val r2TagsString = r2Tags.map(_.toString).getOrElse("No content")
          val fixTagsString = fixTags.toString

          s""""$flexiTagsString", "$r2TagsString", "$fixTagsString""""
        }

        List(
          JSONFileResult(s"${category.toString}-correct-tags-mapping.txt", mapping),
          CSVFileResult(s"${category.toString}-compare-decuplicated-content-mapping.csv", "Flexible, R2, Correct", lines)
        )
      }
    }
  }

  // The list of comparators to apply to data
  val comparators: List[ContentComparator] = List(correctTagMapping)

  def main(args: Array[String]): Unit = {
    new File(PREFIX).mkdir()
    val filePrefix = s"$PREFIX/content"

    // try to open cache file otherwise get data from the databases and cache them
    val dataFuture = Future{FileCache.sourceContentFromDisk(filePrefix)}.recoverWith {
      case NonFatal(e) =>
        R2.init()
        val content = for {
          liveContent <- mongoConnect.liveContent()
          draftContent <- mongoConnect.draftContent()
        } yield {
          Map(ContentCategory.Draft -> draftContent, ContentCategory.Live -> liveContent)
        }
        content.foreach(FileCache.writeContentToDisk(_, filePrefix))
        content
    }

    // apply comparators to data to produce a result
    val result = dataFuture.map { data =>
      // compute known map of old to new tags
      val oldToNewTagIdMap: Map[Long, Long] = computeOldToNewTagIdMap(data)

      comparators.foreach{ comparator =>
        try {
          comparator.compare(data, oldToNewTagIdMap).foreach {
            case CSVFileResult(fileName, header, lines) =>
              val writer = new PrintWriter(s"$PREFIX/$fileName")
              writer.println(header)
              lines.foreach(writer.println)
              writer.close()
            case JSONFileResult(fileName, jsons) =>
              val writer = new PrintWriter(s"$PREFIX/$fileName")
              jsons.foreach(json => writer.println(json.toString()))
              writer.close()
            case ScreenResult(lines) => lines.foreach(System.err.println)
          }
        } catch {
          case NonFatal(e) => System.err.println(s"Error thrown whilst running comparator ${e.printStackTrace()}}")
        }
      }
    }

    Await.result(result, Duration.Inf)

    println("Done")
  }
}
