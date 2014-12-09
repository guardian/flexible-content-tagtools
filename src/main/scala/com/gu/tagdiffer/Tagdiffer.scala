package com.gu.tagdiffer

import java.io.{File, PrintWriter}
import java.util.NoSuchElementException
import com.gu.tagdiffer.component.DatabaseComponent
import com.gu.tagdiffer.index.model.ContentCategory._
import com.gu.tagdiffer.index.model.TagType
import com.gu.tagdiffer.index.model.TagType._
import com.gu.tagdiffer.index.model._
import com.gu.tagdiffer.scaladb.mongoConnect
import play.api.libs.json._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.control.NonFatal


object TagDiffer extends DatabaseComponent {
  // the path where we write result and cache
  val PREFIX = "target/tag-differ"
  /*
   * (Boolea, Int) :
   * - The Boolean value indicates whether tags of the same content are the same but in a different order
   * - The Int value count the number of different tags for the same content
   */
  type orderAndNumberDiff = (Boolean, Int)

  // We can produce two different result so far: Write to a csv file or print to screen
  trait ComparatorResult {
    def lines:Iterable[String]
  }
  case class CSVFileResult(fileName:String, header:String, lines:Iterable[String]) extends ComparatorResult
  case class JSONFileResult(fileName:String, lines:Iterable[String]) extends ComparatorResult
  case class ScreenResult(lines:Iterable[String]) extends ComparatorResult

  trait ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]): Iterable[ComparatorResult]

    def comparePermutations(tagDiff: List[(Option[Tag], Set[Tag])], tagType: TagType): ComparatorResult = {
      val tagDiffCount = tagDiff.groupBy(tag => tag).mapValues(_.size)
      val header = "Flexible, R2, Count"
      val lines = tagDiffCount.toList.sortBy(_._2).reverse map { case ((flex, r2), count) =>
        s"${flex.map(_.internalName)}, ${r2.map(_.internalName).mkString(";")}, $count"
      }
      CSVFileResult(s"${ContentCategory.toString()}_${tagType.toString}_tag_report.csv", header, lines)
    }

    def compareAndMapDifferentTagIds(deltas: List[(Set[Tag],Set[Tag], String)], name:String): ComparatorResult = {
      val r2Tags = deltas.flatMap(_._1).toSet
      val flexiTags = deltas.flatMap(_._2).toSet

      val result = deltas.flatMap { e =>
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
      }.toSet

      val header = "Old Tag Id, Updated Tag Id, Old Internal Name, Updated Internal Name, Old External Name, Updated External Name, " +
        "Old Type, Updated Type, Old Section Id, Updated Section Id, Old Section Name, Updated Section Name"

      val lines = result map { t =>
        val r2t = r2Tags.find(_.tagId == t._2).get
        val ftOpt = flexiTags.find(tag => (tag.tagId == t._1) && (tag.section.id != r2t.section.id)).map(tg => tg)

        val aLine = if (ftOpt.isDefined) {
          val ft = ftOpt.get
          s"${ft.tagId}, ${r2t.tagId}, ${ft.internalName}, ${r2t.internalName}, ${ft.externalName}, ${r2t.externalName}, " +
            s"${ft.tagType}, ${r2t.tagType}, ${ft.section.id}, ${r2t.section.id}, ${ft.section.name}, ${r2t.section.name}"
        } else " "

        aLine
      }

      lines.filterNot(_ != " ")
      CSVFileResult(s"$name.csv", header, lines)
    }

    def compareDeltas(deltas: List[(Set[Tag],Set[Tag], String)], name:String): ComparatorResult = {
      val deltaCount = deltas.groupBy(delta => (delta._1, delta._2)).mapValues(v => (v.size, v.head._3)).toList.sortBy(_._2).reverse
      val lines = deltaCount.map { case ((r2, flex), (count, contentID)) =>
        val r2Tags = r2.toList.sortBy(_.internalName).map(_.toString.mkString("\"", "\n", "\""))
        val flexTags = flex.toList.sortBy(_.internalName).map(_.toString.mkString("\"", "\n", "\""))
        s"$r2Tags,$flexTags,$count, ${Config.composerContentPrefix}$contentID"
      }
      CSVFileResult(s"$name-delta-report.csv", "Only R2, Only Flex, Count, Example Content Id", lines)
    }
  }

  val summaryDiff = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]) = {
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
    def compare(contentMap: Map[Category, List[Content]]) = {
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
    def compare(contentMap: Map[Category, List[Content]]) = {
      contentMap.map { case (category, content) =>
        val pubTagDiff = content.filter(_.isDifferent(_.publications)).map { c =>
          (c.flexiTags.publications.headOption, c.r2Tags.publications.toSet)
        }
        comparePermutations(pubTagDiff, TagType.Publication)
      }
    }
  }

  val bookTagDiffs = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]) = {
      contentMap.map { case (category, content) =>
        val bookTagDiff = content.filter(_.isDifferent(_.book)).map { c =>
          (c.flexiTags.book.headOption, c.r2Tags.book.toSet)
        }
        comparePermutations(bookTagDiff, TagType.Book)
      }
    }
  }

  val sectionTagDiffs = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]) = {
      contentMap.map { case (category, content) =>
        val sectionTagDiff = content.filter(_.isDifferent(_.bookSection)).map { c =>
          (c.flexiTags.bookSection.headOption, c.r2Tags.bookSection.toSet)
        }
        comparePermutations(sectionTagDiff, TagType.BookSection)
      }
    }
  }

  val flexiNewspaperDuplication = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]) = {
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

  def r2FlexDiffTuples(contentList: List[Content]) = contentList.map { content =>
    val r2TagSet = content.r2Tags.allTags.toSet
    val flexTagSet = content.flexiTags.allTags.toSet
    val onlyR2 = r2TagSet diff flexTagSet
    val onlyFlex = flexTagSet diff r2TagSet
    (onlyR2, onlyFlex, content.contentId)
  }

  val setOfDeltas = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]): Iterable[ComparatorResult] = {
      contentMap.map { case (category, contentList) =>
        val deltas = r2FlexDiffTuples(contentList)
        compareDeltas(deltas, s"all-tags-${category.toString}")
      }
    }
  }

  val setOfDeltasWithoutRemappedTags = new ContentComparator {
    def compare(contentMap: Map[Category, List[Content]]): Iterable[ComparatorResult] = {
      val deltas = contentMap.flatMap { case (category, contentList) =>
        r2FlexDiffTuples(contentList).filterNot { case (r2Only, flexOnly, contentId) =>
            r2Only.map(_.copy(tagId = 0)) == flexOnly.map(_.copy(tagId = 0))
        }
      }
      Some(compareAndMapDifferentTagIds(deltas.toList, s"updated-tags-map"))
    }
  }
  // The list of comparators to apply to data
  val comparators:List[ContentComparator] = List(summaryDiff, flexiNewspaperDuplication, publicationTagDiffs, bookTagDiffs, sectionTagDiffs, setOfDeltas, setOfDeltasWithoutRemappedTags)

  implicit val ContentCategoryFormats = new Format[Category] {
    def reads(json: JsValue): JsResult[Category] = json match {
      case JsString(value) =>
        try {
          val enum = ContentCategory.withName(value)
          JsSuccess(enum)
        } catch {
          case e:NoSuchElementException =>
            JsError(s"Invalid enum value: $value")
        }
      case _ => JsError(s"Invalid type for enum value (expected JsString)")
    }

    def writes(o: Category): JsValue = JsString(o.toString)
  }

  implicit val TagTypeFormats = new Format[TagType] {
    def reads(json: JsValue): JsResult[TagType] = json match {
      case JsString(value) =>
        try {
          val enum = TagType.withName(value)
          JsSuccess(enum)
        } catch {
          case e:NoSuchElementException =>
            JsError(s"Invalid enum value: $value")
        }
      case _ => JsError(s"Invalid type for enum value (expected JsString)")
    }

    def writes(o: TagType): JsValue = JsString(o.toString)
  }

  implicit val SectionFormats = Json.format[Section]
  implicit val TagFormats = Json.format[Tag]
  implicit val R2TagsFormats = Json.format[R2Tags]
  implicit val FlexiTagsFormats = Json.format[FlexiTags]
  implicit val ContentFormats = Json.format[Content]
  // Write data to cache file as a Json
  def writeContentToDisk(content: Map[Category, List[Content]], filePrefix: String): Unit = {
    content.foreach { case (key, value) => key.toString -> value
      val file = s"$filePrefix-$key.cache"
      System.err.println(s"Writing content to $file")
      val writer = new PrintWriter(file)
      value.foreach { item =>
        writer.println(Json.stringify(Json.toJson(item)))
      }
      writer.close()
      System.err.println(s"Finished writing content to $file")
    }
  }
  // Get data from cache file
  def sourceContentFromDisk(filePrefix: String): Map[Category, List[Content]] = {
    val categories: Set[Category] = ContentCategory.values.toSet
    categories.map { category =>
      val file = s"$filePrefix-${category.toString}.cache"
      System.err.println(s"Attempting to read and parse content from $file")
      val contentLines = Source.fromFile(file).getLines()
      val content = contentLines.flatMap { line =>
        Json.fromJson[Content](Json.parse(line)).asOpt
      }
      val contentList = content.toList
      System.err.println(s"Successfully read and parsed content from $file")
      category -> contentList
    }.toMap
  }

  def main(args: Array[String]): Unit = {
    new File(PREFIX).mkdir()
    val filePrefix = s"$PREFIX/content"

    // try to open cache file otherwise get data from the databases and cache them
    val dataFuture = Future{sourceContentFromDisk(filePrefix)}.recoverWith {
      case NonFatal(e) =>
        R2.init()
        val content = for {
          liveContent <- mongoConnect.liveContent()
          draftContent <- mongoConnect.draftContent()
        } yield {
          Map(ContentCategory.Draft -> draftContent, ContentCategory.Live -> liveContent)
        }
        content.foreach(writeContentToDisk(_, filePrefix))
        content
    }

    // apply comparators to data a produce a result
    val result = dataFuture.map { data =>
      comparators.foreach{ comparator =>
        try {
          comparator.compare(data).foreach {
            case CSVFileResult(fileName, header, lines) =>
              val writer = new PrintWriter(s"$PREFIX/$fileName")
              writer.println(header)
              lines.foreach(writer.println)
              writer.close()
            case JSONFileResult(fileName, lines) =>
              val writer = new PrintWriter(s"$PREFIX/$fileName")
              lines.foreach(writer.println)
              writer.close()
            case ScreenResult(lines) => lines.foreach(System.err.println)
          }
        } catch {
          case NonFatal(e) => System.err.println(s"Error thrown whilst running comparator}")
        }
      }
    }

    Await.result(result, Duration.Inf)

    println("Done")
  }
}
