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
      case RunAllScrapers =>
        runAllScrapers()
        this
      case WrappedJobsScraped(jobsScraped) =>
        logger.debug(s"Scraped ${jobsScraped.scrapedJobs.size} jobs")
        this
      case ParseLocalJobSources(rawJobSourceName, start, end) =>
        logger.info(s"Start parsing from local job source: $rawJobSourceName")
        parseLocalJobSources(rawJobSourceName, start, end)
        this
    }
  }

  private def parseLocalJobSources(jobSource: RawJobSourceName, startTs: Option[String], endTs: Option[String]): Unit = {
    getScraper(jobSource) ! Scraper.ParseLocalRawJobSources(startTs, endTs)
  }

  private def getScraper(jobSource: RawJobSourceName) = {
    scrapers.get(jobSource) match {
      case Some(s) => s
      case None => addScraper(jobSource)
    }
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

  private def scraperSources = scrapers.keys.mkString(", ")

  private def runAllScrapers(): Unit = {
    logger.info("Run all scrapers")

    if (scrapers.isEmpty) {
      logger.info("Found no scrapers. Creating them...")

      scrapers += RawJobSourceName.ItViec -> addScraper(RawJobSourceName.ItViec)

      logger.info(s"Created scrapers for job source: $scraperSources")
    }

    logger.info(s"Sending messages to start scraping from sources: $scraperSources...")
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

}
