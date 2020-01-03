package vn.johu.scraping.scrapers

import scala.collection.mutable

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.scraping.jsoup.JSoup
import vn.johu.scraping.models.RawJobSourceName
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.scrapers.Scraper.ScrapeResult
import vn.johu.utils.Logging

class JobScraperManager(context: ActorContext[JobScraperManager.Command])
  extends AbstractBehavior[JobScraperManager.Command] with Logging {

  import JobScraperManager._

  private var scrapers = mutable.Map.empty[RawJobSourceName, ActorRef[Scraper.Command]]

  private val jobsScrapedAdapter = context.messageAdapter[ScrapeResult](WrappedScrapeResult.apply)

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Init =>
        init()
        this
      case ScrapeAllSources =>
        scrapeFromAllSources()
        this
      case WrappedScrapeResult(scrapeResult) =>
        logger.debug(s"Scraped ${scrapeResult.newJobs.size} jobs")
        this
      case ParseRawJobSources(rawJobSourceName, start, end) =>
        parseLocalJobSources(rawJobSourceName, start, end)
        this
      case ScrapeFromSources(jobSources, endPage) =>
        scrapeFromSources(jobSources, endPage)
        this
    }
  }

  private def init(): Unit = {
    Set(RawJobSourceName.ItViec, RawJobSourceName.VietnamWorks).foreach(addScraper)
    logger.info("ScraperManager initialized.")
  }

  private def scrapeFromSources(jobSources: List[RawJobSourceName.RawJobSourceName], endPage: Option[Int]): Unit = {
    if (jobSources.isEmpty) {
      logger.info("No sources passed. Run all scrapers.")
      scrapeFromAllSources()
    } else {
      logger.info(s"Running scrapers from sources: ${jobSources.mkString(",")}")
      jobSources.foreach { source =>
        getScraper(source) ! Scraper.Scrape(endPage = endPage, replyTo = jobsScrapedAdapter)
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
        context.spawn[Scraper.Command](ItViecJobScraper(JSoup), "ItViecScraper")
      case RawJobSourceName.VietnamWorks =>
        context.spawn[Scraper.Command](VietnamWorksJobScraper(), "VietnamWorksScraper")
      case _ =>
        throw new IllegalArgumentException(s"Scraper for source $jobSource not supported yet.")
    }

    scrapers += jobSource -> newScraper

    logger.info(s"Scraper for $jobSource added.")

    newScraper
  }

  private def scrapeFromAllSources(): Unit = {
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

object JobScraperManager {

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command](ctx => new JobScraperManager(ctx))
  }

  sealed trait Command

  case object ScrapeAllSources extends Command

  case class ScrapeFromSources(jobSources: List[RawJobSourceName], endPage: Option[Int] = None) extends Command

  case class WrappedScrapeResult(scrapeResult: ScrapeResult) extends Command

  case class ParseRawJobSources(
    jobSourceName: RawJobSourceName,
    scrapingStartTs: Option[String],
    scrapingEndTs: Option[String]
  ) extends Command

  case object Init extends Command

}
