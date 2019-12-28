package vn.johu.scraping

import scala.collection.mutable

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.scraping.Scraper.JobsScraped
import vn.johu.scraping.itviec.ItViecScraper
import vn.johu.scraping.jsoup.JSoup
import vn.johu.scraping.models.RawJobSourceName
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.utils.Logging

class ScraperManager(context: ActorContext[ScraperManager.Command])
  extends AbstractBehavior[ScraperManager.Command] with Logging {

  import ScraperManager._

  private var scrapers = mutable.Map.empty[RawJobSourceName, ActorRef[Scraper.Command]]

  private val jobsScrapedAdapter = context.messageAdapter[JobsScraped](WrappedJobsScraped.apply)

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Init =>
        init()
        this
      case RunAllScrapers =>
        runAllScrapers()
        this
      case WrappedJobsScraped(jobsScraped) =>
        logger.debug(s"Scraped ${jobsScraped.scrapedJobs.size} jobs")
        this
      case ParseLocalJobSources(rawJobSourceName, start, end) =>
        parseLocalJobSources(rawJobSourceName, start, end)
        this
      case RunScrapers(jobSources) =>
        runScrapers(jobSources)
        this
    }
  }

  private def init(): Unit = {
    logger.info("Initializing ScraperManager...")
    Set(RawJobSourceName.ItViec).foreach(addScraper)
  }

  private def runScrapers(jobSources: List[RawJobSourceName.RawJobSourceName]): Unit = {
    if (jobSources.isEmpty) {
      logger.info("No sources passed. Run all scrapers.")
      runAllScrapers()
    } else {
      logger.info(s"Running scrapers from sources: ${jobSources.mkString(",")}")
      jobSources.foreach { source =>
        getScraper(source) ! Scraper.Scrape(replyTo = jobsScrapedAdapter)
      }
    }
  }

  private def parseLocalJobSources(jobSource: RawJobSourceName, startTs: Option[String], endTs: Option[String]): Unit = {
    logger.info(s"Start parsing from local job source: $jobSource...")
    getScraper(jobSource) ! Scraper.ParseLocalRawJobSources(startTs, endTs)
  }

  private def getScraper(jobSource: RawJobSourceName) = {
    scrapers.getOrElse(jobSource, throw new IllegalArgumentException(s"Scraper for source $jobSource not supported yet."))
  }

  private def addScraper(jobSource: RawJobSourceName) = {
    val newScraper = jobSource match {
      case RawJobSourceName.ItViec =>
        context.spawn[Scraper.Command](ItViecScraper(JSoup), "ItViecScraper")
      case _ =>
        throw new IllegalArgumentException(s"Scraper for source $jobSource not supported yet.")
    }

    scrapers += jobSource -> newScraper

    newScraper
  }

  private def runAllScrapers(): Unit = {
    logger.info(s"Running ${scrapers.size} scrapers: ${scrapers.keys.mkString(", ")}")
    scrapers.foreach { scraper =>
      scraper._2 ! Scraper.Scrape(replyTo = jobsScrapedAdapter)
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("ScraperManager stopped.")
      this
  }
}

object ScraperManager {

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command](ctx => new ScraperManager(ctx))
  }

  sealed trait Command

  case object RunAllScrapers extends Command

  case class WrappedJobsScraped(jobsScraped: JobsScraped) extends Command

  case class ParseLocalJobSources(
    jobSourceName: RawJobSourceName,
    scrapingStartTs: Option[String],
    scrapingEndTs: Option[String]
  ) extends Command

  case class RunScrapers(jobSources: List[RawJobSourceName]) extends Command

  case object Init extends Command

}
