package vn.johu.scraping.scrapers

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.circe.{Encoder, Json}
import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.DocRepo
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models.ScrapedJob.Fields
import vn.johu.scraping.models._
import vn.johu.utils.{Configs, DateUtils, Logging}

abstract class Scraper(
  context: ActorContext[Scraper.Command],
  timer: TimerScheduler[Scraper.Command]
) extends AbstractBehavior[Scraper.Command] with Logging {

  import Scraper._

  implicit val ec: ExecutionContextExecutor = context.system.executionContext

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Scrape(page, endPage, replyTo) =>
        scrape(page, endPage, replyTo)
        this
      case ParseLocalRawJobSources(startTs, endTs) =>
        parseLocalRawJobSources(startTs, endTs)
        this
    }
  }

  private def parseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) = {
    val parseResultF =
      DocRepo.findRawJobSources(rawJobSourceName, startTs, endTs).flatMap { sources =>
        logger.info(s"Found ${sources.size} job sources.")
        Future.traverse(sources) { source =>
          val parsingResult = parseJobsFromRaw(source)
          for {
            (existingJobs, newJobs) <- partitionExistingAndNewJobs(parsingResult.jobs)
            _ <- saveParsingResults(existingJobs, newJobs, parsingResult.errors)
            _ <- publishJobs(newJobs)
          } yield {
            logger.info(
              s"""Parse job source [${source.id.get.stringify}] result:
                 |nExistingJobs=${existingJobs.size}, nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}.
                 |""".stripMargin)
            ScrapingResult(existingJobs = existingJobs, newJobs = newJobs, errors = parsingResult.errors)
          }
        }
      }

    parseResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when parsing from local job sources with params: startTs=$startTs, endTs=$endTs.", ex)
      case Success(results) =>
        val result = results.foldLeft((0, 0, 0)) { (acc, elem) =>
          (acc._1 + elem.existingJobs.size, acc._2 + elem.newJobs.size, acc._3 + elem.errors.size)
        }
        logger.info(
          s"""Success parsing from local job sources with params: startTs=$startTs, endTs=$endTs.
             |Result: nJobsUpdated=${result._1}, nJobsInserted=${result._2}, nErrors=${result._3}.
             |""".stripMargin
        )
    }

    parseResultF
  }

  protected def rawJobSourceName: RawJobSourceName

  private def partitionExistingAndNewJobs(jobs: List[ScrapedJob]) = {
    if (jobs.isEmpty) {
      Future.successful((Nil, Nil))
    } else {
      for {
        jobsInDb <- DocRepo.findJobs(jobs.map(_.url).toSet, rawJobSourceName, Set(ScrapedJob.Fields.url))
      } yield {
        val jobsInDbByUrl = jobsInDb.map(j => j.getAsOpt[String](ScrapedJob.Fields.url).get -> j).toMap
        val (existingJobs, newJobs) = jobs.partition(j => jobsInDbByUrl.contains(j.url))
        val existingJobsWithIds = existingJobs.map { j =>
          j.copy(id = jobsInDbByUrl(j.url).getAsOpt[BSONObjectID](ScrapedJob.Fields.id))
        }
        (existingJobsWithIds, newJobs)
      }
    }
  }

  private def saveParsingResults(
    existingJobs: List[ScrapedJob],
    newJobs: List[ScrapedJob],
    errors: List[JobParsingError]
  ) = {
    Future.sequence(
      List(
        DocRepo.updateJobs(existingJobs),
        DocRepo.insertJobs(newJobs),
        DocRepo.insertErrors(errors)
      )
    ).map(_ => ())
  }

  private def scrape(page: Int, endPage: Option[Int], replyTo: ActorRef[Scraper.ScrapeResult]) = {
    logger.info(s"Start scraping at page $page...")

    val startTime = DateUtils.now()

    val scrapeResultF =
      for {
        rawJobSourceContent <- getRawJobSourceContent(page)
        rawJobSource <- DocRepo.insertRawJobSource(rawJobSourceName, page, rawJobSourceContent)
        parsingResult <- Future.successful(parseJobsFromRaw(rawJobSource))
        (existingJobs, newJobs) <- partitionExistingAndNewJobs(parsingResult.jobs)
        shouldScheduleNext <- Future.successful(shouldScheduleNextScraping(page, endPage, newJobs))
        _ <- saveParsingResults(existingJobs, newJobs, parsingResult.errors)
        _ <- publishJobs(newJobs)
        _ <- saveScrapingHistory(rawJobSource, existingJobs, newJobs, parsingResult.errors, startTime, DateUtils.now())
        _ <- Future.successful(scheduleNextScraping(shouldScheduleNext, page, endPage, replyTo))
      } yield {
        logger.info(s"Scrape result: nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}")
        ScrapingResult(existingJobs = existingJobs, newJobs = newJobs, errors = parsingResult.errors)
      }

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from page $page", ex)
      case Success(_) =>
        logger.info(s"Successfully scraped from page: $page")
    }

    for {scrapeResult <- scrapeResultF} yield {
      replyTo ! Scraper.ScrapeResult(page = page, newJobs = scrapeResult.newJobs, existingJobs = scrapeResult.existingJobs)
    }
  }

  private def saveScrapingHistory(
    rawJobSource: RawJobSource,
    existingJobs: List[ScrapedJob],
    newJobs: List[ScrapedJob],
    errors: List[JobParsingError],
    startTime: LocalDateTime,
    endTime: LocalDateTime
  ): Future[Unit] = {
    DocRepo.insertScrapingHistory(
      JobScrapingHistory(
        id = BSONObjectID.generate(),
        rawJobSourceId = rawJobSource.id.get,
        rawJobSourceName = rawJobSource.sourceName,
        newJobIds = newJobs.flatMap(_.id),
        existingJobIds = existingJobs.flatMap(_.id),
        errorIds = errors.map(_.id),
        startTime = BSONDateTime(DateUtils.toMillis(startTime)),
        endTime = BSONDateTime(DateUtils.toMillis(endTime))
      )
    ).map(_ => ())
  }

  protected def getRawJobSourceContent(page: Int): Future[String]

  protected def parseJobsFromRaw(rawJobSource: RawJobSource): JobParsingResult

  private def publishJobs(jobs: List[ScrapedJob]) = {
    implicit val encoder: Encoder[ScrapedJob] = scrapedJobRabbitMqEncoder
    Future.traverse(jobs) { job =>
      RabbitMqClient.publishAsync(job)
    }
  }

  private def shouldScheduleNextScraping(
    currentPage: Int,
    endPage: Option[Int],
    newJobs: List[ScrapedJob]
  ) = {
    if (newJobs.isEmpty) {
      logger.info("No new jobs found. Skip scheduling next scraping.")
      false
    } else if (endPage.exists(_ <= currentPage)) {
      logger.info(s"Reached end page: currentPage=$currentPage, endPage=${endPage.get}. Skip scheduling next scraping.")
      false
    } else {
      true
    }
  }

  private def scheduleNextScraping(
    shouldSchedule: Boolean,
    currentPage: Int,
    endPage: Option[Int],
    replyTo: ActorRef[Scraper.ScrapeResult]
  ): Unit = {
    if (!shouldSchedule) {
      logger.info("Next scraping skipped.")
    } else {
      val nextPage = currentPage + 1
      val config = context.system.settings.config
      val delay = config.getLong(Configs.ScrapingDelayInMillis)
      timer.startSingleTimer(
        timerKey,
        Scrape(nextPage, endPage, replyTo),
        delay = FiniteDuration(delay, TimeUnit.MILLISECONDS)
      )
      logger.info(s"Next scraping scheduled for page $nextPage.")
    }
  }

  protected def timerKey: Any

}

object Scraper {

  sealed trait Command

  case class Scrape(page: Int = 1, endPage: Option[Int] = None, replyTo: ActorRef[ScrapeResult]) extends Command

  case class ScrapeResult(page: Int, newJobs: List[ScrapedJob], existingJobs: List[ScrapedJob])

  case class ParseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) extends Command

  val scrapedJobRabbitMqEncoder: Encoder[ScrapedJob] = (job: ScrapedJob) => {
    Json.obj(
      Fields.id -> Json.fromString(job.id.get.stringify),
      Fields.url -> Json.fromString(job.url),
      Fields.title -> Json.fromString(job.title),
      Fields.tags -> Json.fromValues(job.tags.map(Json.fromString)),
      Fields.postingDate -> Json.fromLong(job.postingDate.toLong.get),
      Fields.company -> Json.fromString(job.company),
      Fields.locations -> Json.fromValues(job.locations.map(Json.fromString)),
      Fields.rawJobSourceName -> Json.fromString(job.rawJobSourceName.toString)
    )
  }

}

case class ScrapingResult(existingJobs: List[ScrapedJob], newJobs: List[ScrapedJob], errors: List[JobParsingError])