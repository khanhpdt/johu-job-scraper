package vn.johu.scraping.itviec

import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scraping.Scraper.{Command, StartScraping}
import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem, JSoup}
import vn.johu.scraping.models.ScrapedJobField.ScrapedJobField
import vn.johu.scraping.models._
import vn.johu.scraping.{Scraper, ScrapingCoordinator, models}
import vn.johu.utils.{DateUtils, Logging}

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

  private def scrape(replyTo: ActorRef[ScrapingCoordinator.JobsScraped]) = {
    val url = "https://itviec.com/it-jobs?page=1"

    logger.info(s"Start scraping at url: $url")

    val scrapeResultF =
      for {
        htmlDoc <- jSoup.getAsync(url)
        rawJobSourceId <- saveHtml(url, htmlDoc)
        parsingResult <- parseJobsFromHtml(htmlDoc, rawJobSourceId)
        _ <- saveParsingResults(parsingResult)
        _ <- publishSuccessfulJobs(parsingResult.jobs)
        _ <- respond(parsingResult.jobs, replyTo)
        _ <- prepareNextScrape()
      } yield parsingResult

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from url: $url", ex)
      case Success(_) =>
        logger.info(s"Successfully scrape from url: $url")
    }

    scrapeResultF
  }

  private def parseJobsFromHtml(doc: HtmlDoc, rawJobSourceId: BSONObjectID) = {
    val jobElements = doc.select("#search-results #jobs div.job")

    if (jobElements.isEmpty) {
      logger.info("Not job element found.")
      Future.successful(ParsingResult(Nil, Nil))
    } else {
      Future.successful {
        val results = jobElements.map(parseJobElement(_, rawJobSourceId))
        ParsingResult(
          jobs = results.collect { case Right(value) => value },
          errors = results.collect { case Left(value) => value }
        )
      }
    }
  }

  private def saveHtml(url: String, htmlDoc: HtmlDoc): Future[BSONObjectID] = {
    MongoDb.rawJobSourceColl.flatMap { coll =>
      val docId = BSONObjectID.generate
      coll.insert(ordered = false).one[RawJobSource](
        RawJobSource(
          id = Some(docId),
          url = url,
          sourceName = RawJobSourceName.ItViec,
          content = htmlDoc.doc.outerHtml(),
          sourceType = RawJobSourceType.Html
        )
      ).map(_ => docId)
    }
  }

  // if successful, return ScrapedJob
  // if not, return JobParsingError(id, element html, all parsing errors, scrapeTs, rawJobSourceId)
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

    def getUrl = trackError(ScrapedJobField.url) {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
        link <- elem.attr("href")
      } yield s"https://itviec.vn/${link.trim.stripPrefix("/")}"
    }

    def getTitle = trackError(ScrapedJobField.title) {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
      } yield elem.text().trim
    }

    def getTags = trackError(ScrapedJobField.tags) {
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
        BSONDateTime(currentTime.minus(pd._1, pd._2).toInstant.toEpochMilli)
      }
    }

    def getPostingDate = trackError(ScrapedJobField.postingDate) {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .distance-time-job-posted > span")
        d <- parsePostingDate(elem.text())
      } yield d
    }

    def getCompany = trackError(ScrapedJobField.company) {
      for {
        elem <- jobElem.selectFirst(".job_content > .logo > .logo-wrapper > a")
        link <- elem.attr("href")
      } yield link.stripPrefix("/companies/").trim
    }

    def getLocation = trackError(ScrapedJobField.location) {
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

  private def saveParsingResults(parsingResult: ParsingResult) = {
    val insertJobsF = MongoDb.scrapedJobColl.flatMap { coll =>
      coll.insert(ordered = false).many(parsingResult.jobs)
    }

    val insertErrorsF = MongoDb.jobParsingErrorColl.flatMap { coll =>
      coll.insert(ordered = false).many(parsingResult.errors)
    }

    for {
      res1 <- insertJobsF
      res2 <- insertErrorsF
    } yield (res1, res2)
  }

  private def publishSuccessfulJobs(jobs: List[ScrapedJob]) = {
    Future.traverse(jobs) { job =>
      RabbitMqClient.publishAsync(job)
    }
  }

  private def prepareNextScrape() = {
    Future.successful(())
  }

  private def respond(jobs: List[ScrapedJob], replyTo: ActorRef[ScrapingCoordinator.JobsScraped]) = {
    Future.successful {
      replyTo ! ScrapingCoordinator.JobsScraped(jobs)
    }
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

case class ParsingResult(jobs: List[ScrapedJob], errors: List[JobParsingError])