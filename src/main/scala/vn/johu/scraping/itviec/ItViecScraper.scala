package vn.johu.scraping.itviec

import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import reactivemongo.api.Cursor
import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONObjectID, document}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scraping.Scraper.{Command, Scrape}
import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem, JSoup}
import vn.johu.scraping.models.ScrapedJobParsingField.ScrapedJobField
import vn.johu.scraping.models._
import vn.johu.scraping.{Scraper, models}
import vn.johu.utils.{Configs, DateUtils, Logging}

class ItViecScraper(
  context: ActorContext[Scraper.Command],
  jSoup: JSoup,
  timers: TimerScheduler[Scraper.Command]
) extends AbstractBehavior[Scraper.Command] with Logging {

  import ItViecScraper._

  implicit val ec: ExecutionContextExecutor = context.system.executionContext

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Scrape(page, replyTo) =>
        scrape(page, replyTo)
        this
    }
  }

  private def scrape(page: Int, replyTo: ActorRef[Scraper.JobsScraped]) = {
    val url = getPageUrl(page)
    logger.info(s"Start scraping at url: $url")

    val scrapeResultF =
      for {
        htmlDoc <- getHtmlDoc(url)
        rawJobSourceId <- saveHtml(url, htmlDoc)
        parsingResult <- parseJobsFromHtml(htmlDoc, rawJobSourceId)
        newJobs <- filterNewJobs(parsingResult.jobs)
        shouldScheduleNext <- shouldScheduleNextScraping(newJobs)
        _ <- saveParsingResults(newJobs, parsingResult.errors)
        _ <- publishNewJobs(newJobs)
        _ <- respond(page, newJobs, replyTo)
        _ <- scheduleNextScraping(shouldScheduleNext, page, replyTo)
      } yield parsingResult

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from url: $url", ex)
      case Success(_) =>
        logger.info(s"Successfully scraped from url: $url")
    }

    scrapeResultF
  }

  private def getPageUrl(page: Int) = {
    s"$BaseUrl/it-jobs?page=$page"
  }

  private def getHtmlDoc(url: String) = {
    jSoup.getAsync(url)
  }

  private def parseJobsFromHtml(doc: HtmlDoc, rawJobSourceId: BSONObjectID) = {
    logger.info("Parsing jobs from html...")

    val jobElements = doc.select("#search-results #jobs div.job")

    if (jobElements.isEmpty) {
      logger.info("No job element found.")
      Future.successful(ParsingResult(Nil, Nil))
    } else {
      logger.info(s"Found ${jobElements.size} job elements")
      Future.successful {
        val results = jobElements.map(parseJobElement(_, rawJobSourceId))
        val parsingResult = ParsingResult(
          jobs = results.collect { case Right(value) => value },
          errors = results.collect { case Left(value) => value }
        )
        logger.info(s"Parsing result: nJobs=${parsingResult.jobs.size}, nErrors=${parsingResult.errors.size}")
        parsingResult
      }
    }
  }

  private def saveHtml(url: String, htmlDoc: HtmlDoc): Future[BSONObjectID] = {
    logger.info("Saving raw html...")
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
        BSONDateTime(currentTime.minus(pd._1, pd._2).toInstant.toEpochMilli)
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

  private def saveParsingResults(newJobs: List[ScrapedJob], errors: List[JobParsingError]) = {
    logger.info(s"Saving parsing results: nJobs=${newJobs.size}, errors=${errors.size}")

    val insertJobsF =
      if (newJobs.isEmpty) {
        Future.successful(Nil)
      } else {
        MongoDb.scrapedJobColl.flatMap { coll =>
          coll.insert(ordered = false).many(newJobs).map(_ => newJobs)
        }
      }
    val insertErrorsF =
      if (errors.isEmpty) {
        Future.successful(Nil)
      } else {
        MongoDb.jobParsingErrorColl.flatMap { coll =>
          coll.insert(ordered = false).many(errors).map(_ => errors)
        }
      }
    for {
      res1 <- insertJobsF
      res2 <- insertErrorsF
    } yield (res1, res2)
  }

  private def publishNewJobs(jobs: List[ScrapedJob]) = {
    logger.info(s"Publishing ${jobs.size} new jobs...")
    Future.traverse(jobs) { job =>
      RabbitMqClient.publishAsync(job)
    }
  }

  private def filterNewJobs(scrapedJobs: List[ScrapedJob]) = {
    if (scrapedJobs.isEmpty) {
      logger.info("No job has been scraped. So no new job.")
      Future.successful(Nil)
    } else {
      val jobByKey = scrapedJobs.map(j => j.url -> j).toMap
      val jobKeys = jobByKey.keySet
      val existingJobsF = MongoDb.scrapedJobColl.flatMap { coll =>
        coll.find(
          selector = document(
            ScrapedJobField.url -> document("$in" -> jobKeys),
            ScrapedJobField.rawJobSourceName -> RawJobSourceName.ItViec.toString
          ),
          projection = Some(document(ScrapedJobField.url -> 1))
        ).cursor[BSONDocument]().collect[List](jobKeys.size, Cursor.FailOnError[List[BSONDocument]]())
      }

      for {
        existingJobs <- existingJobsF
      } yield {
        val existingJobKeys = existingJobs.map(_.getAsOpt[String](ScrapedJobField.url).get).toSet
        val newJobs = jobByKey.filterNot(kv => existingJobKeys.contains(kv._1)).values.toList
        logger.info(s"Found ${newJobs.size} new jobs among the ${scrapedJobs.size} scraped jobs.")
        newJobs
      }
    }
  }

  private def shouldScheduleNextScraping(newJobs: List[ScrapedJob]) = {
    Future.successful {
      if (newJobs.nonEmpty) {
        logger.info("Found new job in the current page. So will try next page...")
        true
      } else {
        logger.info("Found no new job in the current page. So will not try next page.")
        false
      }
    }
  }

  private def respond(page: Int, jobs: List[ScrapedJob], replyTo: ActorRef[Scraper.JobsScraped]) = {
    replyTo ! Scraper.JobsScraped(page, jobs)
    Future.successful(())
  }

  private def scheduleNextScraping(
    shouldSchedule: Boolean,
    currentPage: Int,
    replyTo: ActorRef[Scraper.JobsScraped]
  ) = {
    if (!shouldSchedule) {
      logger.info("Stop scraping as not scheduled.")
    } else {
      val nextPage = currentPage + 1
      logger.info(s"Scheduling for the next scraping: currentPage=$currentPage, nextPage=$nextPage...")
      val config = context.system.settings.config
      val delay = config.getLong(Configs.ScrapingDelayInMillis)
      timers.startSingleTimer(
        ContinueScraping,
        Scrape(nextPage, replyTo),
        delay = FiniteDuration(delay, TimeUnit.MILLISECONDS)
      )
    }

    Future.successful(())
  }

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

  case object ContinueScraping

}

case class ParsingResult(jobs: List[ScrapedJob], errors: List[JobParsingError])