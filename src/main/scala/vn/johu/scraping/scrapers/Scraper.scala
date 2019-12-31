package vn.johu.scraping.scrapers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.circe.{Encoder, Json}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.DocRepo
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models.ScrapedJob.Fields
import vn.johu.scraping.models._
import vn.johu.utils.{Configs, Logging}

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
            _ <- saveLocalParsingResults(existingJobs, newJobs, parsingResult.errors)
            _ <- publishJobs(existingJobs ++ newJobs)
          } yield {
            logger.info(
              s"""Parse job source [${source.id.get.stringify}] result:
                 |nExistingJobs=${existingJobs.size}, nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}.
                 |""".stripMargin)
            (existingJobs.size, newJobs.size, parsingResult.errors.size)
          }
        }
      }

    parseResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when parsing from local job sources with params: startTs=$startTs, endTs=$endTs.", ex)
      case Success(results) =>
        val result = results.foldLeft((0, 0, 0))((r, elem) => (r._1 + elem._1, r._2 + elem._2, r._3 + elem._3))
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
    for {
      newJobs <- filterNewJobs(jobs)
    } yield {
      val newJobKeys = newJobs.map(_.url).toSet
      val existingJobs = jobs.filterNot(j => newJobKeys.contains(j.url))
      (existingJobs, newJobs)
    }
  }

  private def saveLocalParsingResults(
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

  private def scrape(page: Int, endPage: Option[Int], replyTo: ActorRef[Scraper.JobsScraped]) = {
    logger.info(s"Start scraping at page $page...")

    val scrapeResultF =
      for {
        rawJobSourceContent <- getRawJobSourceContent(page)
        rawJobSource <- DocRepo.insertRawJobSource(rawJobSourceName, page, rawJobSourceContent)
        parsingResult <- Future.successful(parseJobsFromRaw(rawJobSource))
        newJobs <- filterNewJobs(parsingResult.jobs)
        shouldScheduleNext <- Future.successful(shouldScheduleNextScraping(page, endPage, newJobs))
        _ <- saveParsingResults(newJobs, parsingResult.errors)
        _ <- publishJobs(newJobs)
        _ <- Future.successful(respond(page, newJobs, replyTo))
        _ <- Future.successful(scheduleNextScraping(shouldScheduleNext, page, endPage, replyTo))
      } yield {
        logger.info(s"Scrape result: nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}")
        parsingResult
      }

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from page $page", ex)
      case Success(_) =>
        logger.info(s"Successfully scraped from page: $page")
    }

    scrapeResultF
  }

  protected def getRawJobSourceContent(page: Int): Future[String]

  protected def parseJobsFromRaw(rawJobSource: RawJobSource): JobParsingResult

  private def saveParsingResults(newJobs: List[ScrapedJob], errors: List[JobParsingError]) = {
    val insertJobsF = DocRepo.insertJobs(newJobs)
    val insertErrorsF = DocRepo.insertErrors(errors)
    for {
      res1 <- insertJobsF
      res2 <- insertErrorsF
    } yield (res1, res2)
  }

  private def publishJobs(jobs: List[ScrapedJob]) = {
    implicit val encoder: Encoder[ScrapedJob] = scrapedJobRabbitMqEncoder
    Future.traverse(jobs) { job =>
      RabbitMqClient.publishAsync(job)
    }
  }

  private def filterNewJobs(scrapedJobs: List[ScrapedJob]) = {
    if (scrapedJobs.isEmpty) {
      Future.successful(Nil)
    } else {
      val jobByUrl = scrapedJobs.map(j => j.url -> j).toMap
      for {
        existingJobs <- DocRepo.findJobs(jobByUrl.keySet, rawJobSourceName)
      } yield {
        val existingJobUrls = existingJobs.map(_.getAsOpt[String](ScrapedJob.Fields.url).get).toSet
        val newJobs = jobByUrl.filterNot(kv => existingJobUrls.contains(kv._1)).values.toList
        newJobs
      }
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

  private def respond(page: Int, jobs: List[ScrapedJob], replyTo: ActorRef[Scraper.JobsScraped]): Unit = {
    replyTo ! Scraper.JobsScraped(page, jobs)
  }

  private def scheduleNextScraping(
    shouldSchedule: Boolean,
    currentPage: Int,
    endPage: Option[Int],
    replyTo: ActorRef[Scraper.JobsScraped]
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

  case class Scrape(page: Int = 1, endPage: Option[Int] = None, replyTo: ActorRef[JobsScraped]) extends Command

  case class JobsScraped(page: Int, scrapedJobs: List[ScrapedJob])

  case class ParseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) extends Command

  val scrapedJobRabbitMqEncoder: Encoder[ScrapedJob] = (job: ScrapedJob) => {
    Json.obj(
      Fields.id -> Json.fromString(job.id.get.stringify),
      Fields.url -> Json.fromString(job.url),
      Fields.title -> Json.fromString(job.title),
      Fields.tags -> Json.fromValues(job.tags.map(Json.fromString)),
      Fields.postingDate -> Json.fromLong(job.postingDate.toLong.get),
      Fields.company -> Json.fromString(job.company),
      Fields.locations -> Json.fromValues(job.locations.map(Json.fromString))
    )
  }

}
