package vn.johu.scraping.scrapers

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
      case req: ScrapePages =>
        context.self ! ScrapePeriodically(
          startPage = req.startPage,
          endPage = req.endPage,
          currentResult = ScrapePagesResult(req.startPage, req.endPage, rawJobSourceName, Nil, Nil),
          replyTo = req.replyTo
        )
        this
      case req: ScrapePeriodically =>
        scrapePeriodically(req)
        this
      case ParseLocalRawJobSources(startTs, endTs) =>
        parseLocalRawJobSources(startTs, endTs)
        this
    }
  }

  private def parseLocalRawJobSources(startTs: Option[String], endTs: Option[String]): Future[List[PageScrapingResult]] = {
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
            PageScrapingResult(existingJobs = existingJobs, newJobs = newJobs, errors = parsingResult.errors)
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

  private def partitionExistingAndNewJobs(jobs: List[ScrapedJob]): Future[(List[ScrapedJob], List[ScrapedJob])] = {
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
  ): Future[Unit] = {
    Future.sequence(
      List(
        DocRepo.updateJobs(existingJobs),
        DocRepo.insertJobs(newJobs),
        DocRepo.insertErrors(errors)
      )
    ).map(_ => ())
  }

  private def scrapePeriodically(req: ScrapePeriodically): Future[Unit] = {
    logger.info(s"Start scraping at page ${req.startPage}...")

    markScrapingStart().flatMap { scrapingHistory =>
      logger.info(s"Created a new scraping record ${scrapingHistory.id}.")
      val scrapeResultF =
        for {
          rawJobSourceContent <- getRawJobSourceContent(req.startPage)
          rawJobSource <- DocRepo.insertRawJobSource(rawJobSourceName, req.startPage, rawJobSourceContent)
          parsingResult <- Future.successful(parseJobsFromRaw(rawJobSource))
          (existingJobs, newJobs) <- partitionExistingAndNewJobs(parsingResult.jobs)
          isDone <- Future.successful(isScrapingDone(req.startPage, req.endPage, newJobs))
          _ <- saveParsingResults(existingJobs, newJobs, parsingResult.errors)
          _ <- publishJobs(newJobs)
        } yield {
          logger.info(s"Scrape result: nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}")
          (isDone, rawJobSource, PageScrapingResult(existingJobs = existingJobs, newJobs = newJobs, errors = parsingResult.errors))
        }

      scrapeResultF.onComplete {
        case Failure(ex) =>
          logger.error(s"Error when scraping from page ${req.startPage}", ex)
          markScrapingEnd(scrapingHistory.copy(scrapingError = Some(ex.getMessage)))
        case Success((_, rawJobSource, result)) =>
          logger.info(s"Successfully scraped from page: ${req.startPage}")
          markScrapingEnd(
            scrapingHistory.copy(
              rawJobSourceId = rawJobSource.id,
              newJobIds = result.newJobs.flatMap(_.id),
              existingJobIds = result.existingJobs.flatMap(_.id),
              parsingErrorIds = result.errors.map(_.id)
            )
          )
      }

      for {
        (isDone, _, pageScrapingResult) <- scrapeResultF
      } yield {
        val currentResult = req.currentResult.copy(
          newJobs = req.currentResult.newJobs ++ pageScrapingResult.newJobs,
          existingJobs = req.currentResult.existingJobs ++ pageScrapingResult.existingJobs,
        )
        if (!isDone) {
          scheduleNextScraping(req, currentResult)
        } else {
          logger.info("Scraping done.")
          req.replyTo ! currentResult
        }
      }
    }
  }

  private def markScrapingStart(): Future[JobScrapingHistory] = {
    val history = JobScrapingHistory(
      id = BSONObjectID.generate(),
      rawJobSourceName = rawJobSourceName,
      startTime = BSONDateTime(DateUtils.nowMillis())
    )
    DocRepo.insertScrapingHistory(history)
  }

  private def markScrapingEnd(scrapingHistory: JobScrapingHistory): Future[Unit] = {
    DocRepo.saveScrapingHistory(
      scrapingHistory.copy(endTime = Some(BSONDateTime(DateUtils.nowMillis())))
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

  private def isScrapingDone(currentPage: Int, endPage: Option[Int], newJobs: List[ScrapedJob]) = {
    if (newJobs.isEmpty) {
      logger.info("No new jobs found. Skip scheduling next scraping.")
      true
    } else if (endPage.exists(_ <= currentPage)) {
      logger.info(s"Reached end page: currentPage=$currentPage, endPage=${endPage.get}. Skip scheduling next scraping.")
      true
    } else {
      false
    }
  }

  private def scheduleNextScraping(req: ScrapePeriodically, currentResult: ScrapePagesResult): Unit = {
    val nextReq = req.copy(startPage = req.startPage + 1, currentResult = currentResult)
    val config = context.system.settings.config
    val delay = FiniteDuration(config.getLong(Configs.ScrapingDelayInMillis), TimeUnit.MILLISECONDS)
    timer.startSingleTimer(
      timerKey,
      nextReq,
      delay = delay
    )
    logger.info(s"Next scraping for page ${nextReq.startPage} will be run in ${delay.toSeconds} seconds.")
  }

  protected def timerKey: Any

}

object Scraper {

  sealed trait Command

  case class ScrapePages(
    startPage: Int = 1,
    endPage: Option[Int] = None,
    replyTo: ActorRef[ScrapePagesResult]
  ) extends Command

  private case class ScrapePeriodically(
    startPage: Int = 1,
    endPage: Option[Int] = None,
    currentResult: ScrapePagesResult,
    replyTo: ActorRef[ScrapePagesResult]
  ) extends Command

  case class ScrapePagesResult(
    startPage: Int,
    endPage: Option[Int],
    rawJobSourceName: RawJobSourceName,
    newJobs: List[ScrapedJob],
    existingJobs: List[ScrapedJob]
  )

  case class ParseLocalRawJobSources(scrapingStartTs: Option[String], scrapingEndTs: Option[String]) extends Command

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

case class PageScrapingResult(existingJobs: List[ScrapedJob], newJobs: List[ScrapedJob], errors: List[JobParsingError])
