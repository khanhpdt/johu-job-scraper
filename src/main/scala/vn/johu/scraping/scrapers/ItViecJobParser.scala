package vn.johu.scraping.scrapers

import java.time.temporal.ChronoUnit
import scala.util.matching.Regex

import reactivemongo.api.bson.BSONDateTime

import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem}
import vn.johu.scraping.models._
import vn.johu.scraping.scrapers.ItViecJobScraper.BaseUrl
import vn.johu.utils.DateUtils

object ItViecJobParser extends JobParser {

  private object PostingDatePatterns {
    val Second: Regex = "^(\\d+)\\s*(second|seconds)\\s*ago$".r
    val Minute: Regex = "^(\\d+)\\s*(minute|minutes)\\s*ago$".r
    val Hour: Regex = "^(\\d+)\\s*(hour|hours)\\s*ago$".r
    val Day: Regex = "^(\\d+)\\s*(day|days)\\s*ago$".r
    val Week: Regex = "^(\\d+)\\s*(week|weeks)\\s*ago$".r
    val Month: Regex = "^(\\d+)\\s*(month|months)\\s*ago$".r
  }

  override def parseJobs(rawJobSource: RawJobSource): JobParsingResult = {
    val doc = HtmlDoc.fromHtml(rawJobSource.content)
    val jobElements = doc.select("#search-results #jobs div.job")
    if (jobElements.isEmpty) {
      JobParsingResult(Nil, Nil)
    } else {
      val results = jobElements.map(parseJobElement(_, rawJobSource))
      JobParsingResult(
        jobs = results.collect { case Right(value) => value },
        errors = results.collect { case Left(value) => value }
      )
    }
  }

  private def parseJobElement(jobElem: HtmlElem, rawJobSource: RawJobSource): Either[JobParsingError, ScrapedJob] = {

    def getUrl = {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
        link <- elem.attr("href")
      } yield s"$BaseUrl/${link.trim.stripPrefix("/")}"
    }

    def getTitle = {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
      } yield elem.text().trim
    }

    def getTags = {
      for {
        elems <- jobElem.selectOpt(".job_content > .job__description > .job-bottom > .tag-list > a > span")
      } yield elems.map(_.text().trim).toSet
    }

    def parsePostingDate(dateStr: String): Option[BSONDateTime] = {
      import PostingDatePatterns._
      val parsedDate = dateStr.trim.toLowerCase match {
        case Second(v, _) => Some((v.toInt, ChronoUnit.SECONDS))
        case Minute(v, _) => Some((v.toInt, ChronoUnit.MINUTES))
        case Hour(v, _) => Some((v.toInt, ChronoUnit.HOURS))
        case Day(v, _) => Some((v.toInt, ChronoUnit.DAYS))
        case Week(v, _) => Some((v.toInt, ChronoUnit.WEEKS))
        case Month(v, _) => Some((v.toInt, ChronoUnit.MONTHS))
        case _ => None
      }
      parsedDate.map { pd =>
        val currentTime = DateUtils.now()
        BSONDateTime(DateUtils.toMillis(currentTime.minus(pd._1, pd._2)))
      }
    }

    def getPostingDate = {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .distance-time-job-posted > span")
        d <- parsePostingDate(elem.text())
      } yield d
    }

    def getCompany = {
      for {
        elem <- jobElem.selectFirst(".job_content > .logo > .logo-wrapper > a")
        link <- elem.attr("href")
      } yield link.stripPrefix("/companies/").trim
    }

    def getLocations = {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .city > .address > .text")
      } yield Set(elem.text().trim)
    }

    buildScrapedJob(
      url = getUrl,
      title = getTitle,
      tags = getTags,
      postingDate = getPostingDate,
      company = getCompany,
      locations = getLocations,
      rawJobSource = rawJobSource,
      rawJobElementSource = jobElem.elem.outerHtml()
    )
  }

}
