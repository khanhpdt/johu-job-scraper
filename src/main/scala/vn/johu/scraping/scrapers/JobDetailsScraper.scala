package vn.johu.scraping.scrapers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.persistence.MongoDb
import vn.johu.scraping.jsoup.{HtmlDoc, JSoup}
import vn.johu.scraping.models.{ScrapedJob, ScrapedJobDetails}
import vn.johu.utils.{Configs, DateUtils, Logging}

class JobDetailsScraper(
  context: ActorContext[JobDetailsScraper.Command],
  timer: TimerScheduler[JobDetailsScraper.Command],
  timerKey: Any,
  jSoup: JSoup
) extends AbstractBehavior[JobDetailsScraper.Command] with Logging {

  import JobDetailsScraper._

  implicit val ec: ExecutionContextExecutor = context.system.executionContext

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case req: ScrapeJobDetails =>
        context.self ! ScrapeJobDetailsPeriodically(req.jobs, Nil, req.replyTo)
        this
      case req: ScrapeJobDetailsPeriodically =>
        scrapeJobDetailsPeriodically(req)
        this
    }
  }

  private def scrapeJobDetailsPeriodically(request: ScrapeJobDetailsPeriodically): Unit = {
    logger.info(s"nScraped=${request.scrapedJobDetailsList.size}, nRemaining=${request.remainingJobs.size}")
    request.remainingJobs match {
      case Nil =>
        completeScraping(request.scrapedJobDetailsList, request.replyTo)
      case job :: tail =>
        val scrapingResultF = for {
          htmlTry <- jSoup.tryGetAsync(job.url)
          scrapedJobDetails <- saveScrapedJobDetails(job, htmlTry)
          _ <- Future.successful(scheduleNextScraping(scrapedJobDetails, tail, request))
        } yield ()

        scrapingResultF.onComplete {
          case Failure(exception) =>
            logger.error(s"Failed to scrape job details ${job.id.get}.", exception)
          case Success(_) =>
            logger.info(s"Scraped job at page ${job.url}.")
        }
    }
  }

  private def saveScrapedJobDetails(job: ScrapedJob, htmlTry: Try[HtmlDoc]) = {
    htmlTry match {
      case Success(jobHtml) =>
        saveScrapedJobDetails(job.id.get, html = Some(jobHtml.doc.body().outerHtml()))
      case Failure(exception) =>
        logger.error(s"Failed to get page html at ${job.url}.", exception)
        saveScrapedJobDetails(job.id.get, error = Some(exception.getMessage))
    }
  }

  private def completeScraping(scrapedJobDetails: List[ScrapedJobDetails], replyTo: ActorRef[ScrapeJobDetailsResult]): Unit = {
    logger.info("No job left. Scraping job details done.")
    replyTo ! ScrapeJobDetailsResult(scrapedJobDetails)
  }

  private def scheduleNextScraping(
    scrapedJobDetails: ScrapedJobDetails,
    remainingJobs: List[ScrapedJob],
    request: ScrapeJobDetailsPeriodically
  ): Unit = {
    val allScrapedJobDetails = request.scrapedJobDetailsList ++ List(scrapedJobDetails)
    if (remainingJobs.isEmpty) {
      completeScraping(allScrapedJobDetails, request.replyTo)
    } else {
      val config = context.system.settings.config
      val delay = FiniteDuration(config.getLong(Configs.ScrapingJobDetailsDelayInMillis), TimeUnit.MILLISECONDS)
      timer.startSingleTimer(
        timerKey,
        ScrapeJobDetailsPeriodically(remainingJobs, allScrapedJobDetails, request.replyTo),
        delay = delay
      )
      logger.info(s"Next scraping in ${delay.toSeconds} seconds.")
    }
  }

  private def saveScrapedJobDetails(
    scrapedJobId: BSONObjectID,
    html: Option[String] = None,
    error: Option[String] = None
  ) = {
    MongoDb.scrapedJobDetailsColl.flatMap { coll =>
      val jobDetails = ScrapedJobDetails(
        id = BSONObjectID.generate(),
        scrapedJobId = scrapedJobId,
        html = html,
        scrapingError = error,
        createdTs = BSONDateTime(DateUtils.nowMillis())
      )
      coll.insert(ordered = false).one[ScrapedJobDetails](jobDetails).map(_ => jobDetails)
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("JobDetailScraper stopped")
      this
  }

}

object JobDetailsScraper {

  def apply(jSoup: JSoup = JSoup, timerKey: Any): Behavior[Command] = {
    Behaviors.setup { ctx =>
      Behaviors.withTimers { timer =>
        new JobDetailsScraper(ctx, timer, timerKey, jSoup)
      }
    }
  }

  sealed trait Command

  case class ScrapeJobDetails(
    jobs: List[ScrapedJob],
    replyTo: ActorRef[ScrapeJobDetailsResult]
  ) extends Command

  case class ScrapeJobDetailsResult(jobDetails: List[ScrapedJobDetails])

  private case class ScrapeJobDetailsPeriodically(
    remainingJobs: List[ScrapedJob],
    scrapedJobDetailsList: List[ScrapedJobDetails],
    replyTo: ActorRef[ScrapeJobDetailsResult]
  ) extends Command

}
