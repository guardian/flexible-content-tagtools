package com.gu.tagdiffer.unitTests

import com.gu.tagdiffer.index.model._
import com.gu.tagdiffer.index.model.TagType._
import org.joda.time.DateTime
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.scalatest.matchers.ShouldMatchers

class DiffFunctionTests extends FeatureSpec with GivenWhenThen with ShouldMatchers{
  val section = Section(8, "test section", Some("testSectionPath"), "section")
  val mainTag = Tagging(Tag(1, Other, "tag 2", "tag", section, Some(true)), false)
  val leadTag = Tagging(Tag(2, Other, "tag 3", "tag", section, Some(true)), true)
  val contributorTag = Tagging(Tag(3, Contributor, "tag 4", "tag", section, Some(true)), false)
  val contributorTag2 =Tagging( Tag(4, Contributor, "tag 5", "tag", section, Some(true)), false)
  val publicationTag = Tagging(Tag(5, Publication, "tag 6", "tag", section, Some(true)), false)
  val bookTag = Tagging(Tag(6, Book, "tag 7", "tag", section, Some(true)), false)
  val bookSectionTag = Tagging(Tag(7, BookSection, "tag 8", "tag", section, Some(true)), false)
  val createTimestamp = new DateTime()
  val lastModified = new DateTime()

  feature("Check tags order") {
    scenario("different order") {
      given("the same tags")
        val flexiTags = FlexiTags(List(mainTag, leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
        val r2Tags = R2Tags(List(leadTag, mainTag, contributorTag, publicationTag, bookSectionTag, bookTag))
        val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different order should be true")
      and("different number should be 0")
        content.diffTags(_.allTags) should be((true, 0))
    }

    scenario("same order") {
      given("the same tags")
      val flexiTags = FlexiTags(List(mainTag, leadTag), List(contributorTag), List(publicationTag), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, leadTag, contributorTag, publicationTag, bookTag, bookSectionTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different order should be false")
      and("different number should be 0")
        content.diffTags(_.allTags) should be((false, 0))
    }
  }

  feature("Check tags number") {
    scenario("all flexi and r2 tags are different") {
      given("4 tags for flexi and 2 for R2")
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(mainTag, contributorTag2))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different number should be 6")
      content.diffTags(_.allTags) should be((false, 6))
    }

    scenario("not all flexi and r2 tags are different") {
      given("4 tags for flexi and 2 for R2 with")
      and("both R2 tags are in flexi")
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(), List(bookTag), List(bookSectionTag))
      val r2Tags = R2Tags(List(leadTag, contributorTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different number should be 2")
      content.diffTags(_.allTags) should be((false, 2))
    }

    scenario("Flexi has no tags") {
      given("4 tags for r2")
      val flexiTags = FlexiTags(List(), List(), List(), List(), List())
      val r2Tags = R2Tags(List(leadTag, contributorTag, publicationTag, bookTag))
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different number should be 4")
      content.diffTags(_.allTags) should be((false, 4))
    }

    scenario("R2 has no tags") {
      given("4 tags for flexi")
      val flexiTags = FlexiTags(List(leadTag), List(contributorTag), List(publicationTag), List(bookTag), List())
      val r2Tags = R2Tags(List())
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different number should be 4")
      content.diffTags(_.allTags) should be((false, 4))
    }

    scenario("R2 and Flexi have no tags") {
      val flexiTags = FlexiTags(List(), List(), List(), List(), List())
      val r2Tags = R2Tags(List())
      val content = Content("1", "11", "article", createTimestamp, lastModified, lastModified, flexiTags, r2Tags)
      then("different number should be 0")
      content.diffTags(_.allTags) should be((false, 0))
    }
  }
}
