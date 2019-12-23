package vn.johu.scraping.itviec

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.persistence.MongoDb
import vn.johu.scraping.Scraper.{Command, StartScraping}
import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem, JSoup}
import vn.johu.scraping.models.{RawJobSource, RawJobSourceName, RawJobSourceType, ScrapedJob}
import vn.johu.scraping.{Scraper, ScrapingCoordinator}
import vn.johu.utils.Logging

class ItViecScraper(
  context: ActorContext[Scraper.Command],
  jSoup: JSoup
) extends AbstractBehavior[Scraper.Command] with Logging {

  import ItViecScraper._

  implicit val ec: ExecutionContextExecutor = context.system.executionContext

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case StartScraping(replyTo) =>
        scrape(replyTo)
        this
    }
  }

  private def scrape(replyTo: ActorRef[ScrapingCoordinator.JobsScraped]): Unit = {
    val url = "https://itviec.com/it-jobs?page=1"

    logger.info(s"Start scraping at url: $url")

    val scrapeResultF =
      for {
        htmlDoc <- jSoup.getAsync(url)
      } yield {
        parseDoc(htmlDoc, replyTo)
      }

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from url: $url", ex)
      case Success(_) =>
        logger.info(s"Successfully scrape from url: $url")
    }
  }

  private def parseDoc(doc: HtmlDoc, replyTo: ActorRef[ScrapingCoordinator.JobsScraped]): Unit = {
    val parseResult =
      for {
        rawJobSourceId <- saveHtml(doc)
      } yield {
        val jobElements = doc.select("#search-results #jobs div.job")

        val jobs = jobElements.flatMap(parseJobElement(_, rawJobSourceId))

        prepareNextScrape(jobs, replyTo)

        replyTo ! ScrapingCoordinator.JobsScraped(jobs)
      }

    parseResult.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when parsing", ex)
      case Success(_) =>
        logger.info(s"Parse doc successfully.")
    }
  }

  private def saveHtml(htmlDoc: HtmlDoc): Future[BSONObjectID] = {
    MongoDb.rawJobSourceColl.flatMap { coll =>
      val docId = BSONObjectID.generate
      coll.insert(ordered = false).one[RawJobSource](
        RawJobSource(
          id = Some(docId),
          sourceName = RawJobSourceName.ItViec,
          content = htmlDoc.doc.outerHtml(),
          sourceType = RawJobSourceType.Html
        )
      ).map(_ => docId)
    }
  }

  private def parseJobElement(jobElem: HtmlElem, rawJobSourceId: BSONObjectID): Option[ScrapedJob] = {

    def withLogging[T](key: String)(f: => Option[T]) = {
      val opt = f
      if (opt.isEmpty) {
        logger.error(s"Missing $key")
      }
      opt
    }

    def getUrl = withLogging("url") {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
        link <- elem.attr("href")
      } yield s"https://itviec.vn/${link.trim}"
    }

    def getTitle = withLogging("title") {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
      } yield elem.text().trim
    }

    def getTags = withLogging("tags") {
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
        val currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault())
        BSONDateTime(currentTime.minus(pd._1, pd._2).toInstant.toEpochMilli)
      }
    }

    def getPostingDate = withLogging("postingDate") {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .distance-time-job-posted > span")
        d <- parsePostingDate(elem.text())
      } yield d
    }

    def getCompany = withLogging("company") {
      for {
        elem <- jobElem.selectFirst(".job_content > .logo > .logo-wrapper > a")
        link <- elem.attr("href")
      } yield link.stripPrefix("/companies/").trim
    }

    def getLocation = withLogging("location") {
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
          rawJobSourceId = rawJobSourceId
        )
      }

    if (jobOpt.isEmpty) {
      logger.error(s"Could not parse job element. One of the required field is missing. Job element: ${jobElem.elem.text()}")
    }

    jobOpt
  }

  private def prepareNextScrape(scrapedJobs: List[ScrapedJob], replyTo: ActorRef[ScrapingCoordinator.JobsScraped]): Unit = {

  }

}

object ItViecScraper {
  def apply(jSoup: JSoup): Behavior[Scraper.Command] = {
    Behaviors.setup[Scraper.Command](ctx => new ItViecScraper(ctx, jSoup))
  }

  object PostingDatePatterns {
    val Second: Regex = "^(\\d+)\\s*(second|seconds)\\s*ago$".r
    val Minute: Regex = "^(\\d+)\\s*(minute|minutes)\\s*ago$".r
    val Hour: Regex = "^(\\d+)\\s*(hour|hours)\\s*ago$".r
    val Day: Regex = "^(\\d+)\\s*(day|days)\\s*ago$".r
    val Week: Regex = "^(\\d+)\\s*(week|weeks)\\s*ago$".r
    val Month: Regex = "^(\\d+)\\s*(month|months)\\s*ago$".r
  }

}
