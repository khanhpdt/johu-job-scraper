package vn.johu.scraping.scrapers

import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.matching.Regex

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem, JSoup}
import vn.johu.scraping.models
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models.ScrapedJobParsingField.ScrapedJobField
import vn.johu.scraping.models._
import vn.johu.utils.DateUtils

class ItViecScraper(
  context: ActorContext[Scraper.Command],
  jSoup: JSoup,
  timer: TimerScheduler[Scraper.Command]
) extends Scraper(context, timer) {

  import ItViecScraper._

  override protected def getRawJobSourceContent(page: Int): Future[String] = {
    val pageUrl = s"$BaseUrl/it-jobs?page=$page"
    jSoup.getAsync(pageUrl).map(_.doc.outerHtml())
  }

  override protected def parseJobsFromRaw(rawJobSource: RawJobSource): JobParsingResult = {
    val doc = HtmlDoc.fromHtml(rawJobSource.content)
    val jobElements = doc.select("#search-results #jobs div.job")
    if (jobElements.isEmpty) {
      JobParsingResult(Nil, Nil)
    } else {
      val results = jobElements.map(parseJobElement(_, rawJobSource.id.get))
      JobParsingResult(
        jobs = results.collect { case Right(value) => value },
        errors = results.collect { case Left(value) => value }
      )
    }
  }

  private def parseJobElement(jobElem: HtmlElem, rawJobSourceId: BSONObjectID): Either[JobParsingError, ScrapedJob] = {

    val parsingErrors = mutable.ListBuffer.empty[String]

    def trackError[T](key: ScrapedJobField)(f: => Option[T]) = {
      val opt = f
      if (opt.isEmpty) {
        val error = s"Missing ${key.toString}"
        parsingErrors += error
      }
      opt
    }

    def getUrl = trackError(ScrapedJobParsingField.url) {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
        link <- elem.attr("href")
      } yield s"$BaseUrl/${link.trim.stripPrefix("/")}"
    }

    def getTitle = trackError(ScrapedJobParsingField.title) {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
      } yield elem.text().trim
    }

    def getTags = trackError(ScrapedJobParsingField.tags) {
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

    def getPostingDate = trackError(ScrapedJobParsingField.postingDate) {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .distance-time-job-posted > span")
        d <- parsePostingDate(elem.text())
      } yield d
    }

    def getCompany = trackError(ScrapedJobParsingField.company) {
      for {
        elem <- jobElem.selectFirst(".job_content > .logo > .logo-wrapper > a")
        link <- elem.attr("href")
      } yield link.stripPrefix("/companies/").trim
    }

    def getLocation = trackError(ScrapedJobParsingField.location) {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .city > .address > .text")
      } yield elem.text().trim
    }

    val jobOpt =
      for {
        id <- Some(BSONObjectID.generate())
        url <- getUrl
        title <- getTitle
        tags <- getTags
        postingDate <- getPostingDate
        company <- getCompany
        location <- getLocation
      } yield {
        ScrapedJob(
          id = Some(id),
          url = url,
          title = title,
          tags = tags,
          postingDate = postingDate,
          company = company,
          location = location,
          rawJobSourceName = RawJobSourceName.ItViec,
          rawJobSourceId = rawJobSourceId
        )
      }

    jobOpt match {
      case Some(job) =>
        Right(job)
      case None =>
        val id = BSONObjectID.generate()
        logger.error(s"Could not parse job element. One of the required field is missing. Error id: [${id.stringify}]")
        Left(
          models.JobParsingError(
            id = id,
            rawSourceContent = jobElem.elem.outerHtml(),
            errors = parsingErrors.toList,
            rawJobSourceId = rawJobSourceId,
            scrapingTs = BSONDateTime(DateUtils.nowMillis())
          )
        )
    }
  }

  override protected val rawJobSourceName: RawJobSourceName = RawJobSourceName.ItViec

  override protected val timerKey: Any = TimerKey

}

object ItViecScraper {

  val BaseUrl = "https://itviec.com"

  def apply(jSoup: JSoup): Behavior[Scraper.Command] = {
    Behaviors.setup[Scraper.Command] { ctx =>
      Behaviors.withTimers[Scraper.Command] { timers =>
        new ItViecScraper(ctx, jSoup, timers)
      }
    }
  }

  object PostingDatePatterns {
    val Second: Regex = "^(\\d+)\\s*(second|seconds)\\s*ago$".r
    val Minute: Regex = "^(\\d+)\\s*(minute|minutes)\\s*ago$".r
    val Hour: Regex = "^(\\d+)\\s*(hour|hours)\\s*ago$".r
    val Day: Regex = "^(\\d+)\\s*(day|days)\\s*ago$".r
    val Week: Regex = "^(\\d+)\\s*(week|weeks)\\s*ago$".r
    val Month: Regex = "^(\\d+)\\s*(month|months)\\s*ago$".r
  }

  case object TimerKey

}