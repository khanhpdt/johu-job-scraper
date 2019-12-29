package vn.johu.scraping.scrapers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONObjectID, document}
import reactivemongo.api.{Cursor, WriteConcern}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
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
      case Scrape(page, replyTo) =>
        scrape(page, replyTo)
        this
      case ParseLocalRawJobSources(startTs, endTs) =>
        parseLocalRawJobSources(startTs, endTs)
        this
    }
  }

  private def parseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) = {
    val parseResultF =
      findJobSources(startTs, endTs).flatMap { sources =>
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

  private def findJobSources(startTs: Option[String], endTs: Option[String]) = {
    val startTimeOpt = startTs.map(DateUtils.parseDateTime)
    val endTimeOpt = endTs.map(DateUtils.parseDateTime)

    var query = document(RawJobSource.Fields.sourceName -> rawJobSourceName.toString)
    startTimeOpt.foreach(t => query ++= RawJobSource.Fields.scrapingTs -> document("$gte" -> t))
    endTimeOpt.foreach(t => query ++= RawJobSource.Fields.scrapingTs -> document("$lte" -> t))

    MongoDb.rawJobSourceColl.flatMap { coll =>
      coll.find(query)
        .cursor[RawJobSource]()
        .collect[List](-1, Cursor.FailOnError[List[RawJobSource]]())
    }
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

  private def saveLocalParsingResults(existingJobs: List[ScrapedJob], newJobs: List[ScrapedJob], errors: List[JobParsingError]) = {
    Future.sequence(
      List(
        updateJobs(existingJobs),
        insertJobs(newJobs),
        insertErrors(errors)
      )
    ).map(_ => ())
  }

  private def updateJobs(jobs: List[ScrapedJob]) = {
    if (jobs.isEmpty) {
      Future.successful(Nil)
    } else {
      val updateResultF = MongoDb.scrapedJobColl.flatMap { coll =>
        Future.traverse(jobs) { job =>
          val jobToUpdate = job.copy(id = None)
          coll.findAndUpdate(
            selector = document(ScrapedJob.Fields.url -> jobToUpdate.url),
            update = jobToUpdate,
            fetchNewObject = false,
            upsert = false,
            sort = None,
            fields = None,
            bypassDocumentValidation = false,
            writeConcern = WriteConcern.Acknowledged,
            maxTime = None,
            collation = None,
            arrayFilters = Seq.empty
          )
        }
      }
      updateResultF.map(_ => jobs)
    }
  }

  private def scrape(page: Int, replyTo: ActorRef[Scraper.JobsScraped]) = {
    logger.info(s"Start scraping at page $page...")

    val scrapeResultF =
      for {
        rawJobSourceContent <- getRawJobSourceContent(page)
        rawJobSource <- saveRawJobSource(page, rawJobSourceContent)
        parsingResult <- Future.successful(parseJobsFromRaw(rawJobSource))
        newJobs <- filterNewJobs(parsingResult.jobs)
        shouldScheduleNext <- Future.successful(shouldScheduleNextScraping(newJobs))
        _ <- saveParsingResults(newJobs, parsingResult.errors)
        _ <- publishJobs(newJobs)
        _ <- Future.successful(respond(page, newJobs, replyTo))
        _ <- Future.successful(scheduleNextScraping(shouldScheduleNext, page, replyTo))
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

  private def saveRawJobSource(page: Int, content: String) = {
    MongoDb.rawJobSourceColl.flatMap { coll =>
      val source = RawJobSource(
        id = Some(BSONObjectID.generate),
        page = page,
        sourceName = rawJobSourceName,
        content = content,
        scrapingTs = BSONDateTime(DateUtils.nowMillis())
      )
      coll.insert(ordered = false).one[RawJobSource](source).map(_ => source)
    }
  }

  private def saveParsingResults(newJobs: List[ScrapedJob], errors: List[JobParsingError]) = {
    val insertJobsF = insertJobs(newJobs)
    val insertErrorsF = insertErrors(errors)
    for {
      res1 <- insertJobsF
      res2 <- insertErrorsF
    } yield (res1, res2)
  }

  private def insertJobs(jobs: List[ScrapedJob]) = {
    if (jobs.isEmpty) {
      Future.successful(Nil)
    } else {
      MongoDb.scrapedJobColl.flatMap { coll =>
        coll.insert(ordered = false).many(jobs).map(_ => jobs)
      }
    }
  }

  private def insertErrors(errors: List[JobParsingError]) = {
    if (errors.isEmpty) {
      Future.successful(Nil)
    } else {
      MongoDb.jobParsingErrorColl.flatMap { coll =>
        coll.insert(ordered = false).many(errors).map(_ => errors)
      }
    }
  }

  private def publishJobs(jobs: List[ScrapedJob]) = {
    Future.traverse(jobs) { job =>
      RabbitMqClient.publishAsync(job)
    }
  }

  private def filterNewJobs(scrapedJobs: List[ScrapedJob]) = {
    if (scrapedJobs.isEmpty) {
      Future.successful(Nil)
    } else {
      val jobByKey = scrapedJobs.map(j => j.url -> j).toMap
      val jobKeys = jobByKey.keySet
      val existingJobsF = MongoDb.scrapedJobColl.flatMap { coll =>
        coll.find(
          selector = document(
            ScrapedJob.Fields.url -> document("$in" -> jobKeys),
            ScrapedJob.Fields.rawJobSourceName -> rawJobSourceName.toString
          ),
          projection = Some(document(ScrapedJob.Fields.url -> 1))
        ).cursor[BSONDocument]().collect[List](jobKeys.size, Cursor.FailOnError[List[BSONDocument]]())
      }

      for {
        existingJobs <- existingJobsF
      } yield {
        val existingJobKeys = existingJobs.map(_.getAsOpt[String](ScrapedJob.Fields.url).get).toSet
        val newJobs = jobByKey.filterNot(kv => existingJobKeys.contains(kv._1)).values.toList
        newJobs
      }
    }
  }

  private def shouldScheduleNextScraping(newJobs: List[ScrapedJob]) = newJobs.nonEmpty

  private def respond(page: Int, jobs: List[ScrapedJob], replyTo: ActorRef[Scraper.JobsScraped]): Unit = {
    replyTo ! Scraper.JobsScraped(page, jobs)
  }

  private def scheduleNextScraping(
    shouldSchedule: Boolean,
    currentPage: Int,
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
        Scrape(nextPage, replyTo),
        delay = FiniteDuration(delay, TimeUnit.MILLISECONDS)
      )
      logger.info(s"Next scraping scheduled for page $nextPage.")
    }
  }

  protected def timerKey: Any

}

object Scraper {

  sealed trait Command

  case class Scrape(page: Int = 1, replyTo: ActorRef[JobsScraped]) extends Command

  case class JobsScraped(page: Int, scrapedJobs: List[ScrapedJob])

  case class ParseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) extends Command

}
