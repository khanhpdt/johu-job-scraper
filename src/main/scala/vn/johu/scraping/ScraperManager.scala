package vn.johu.scraping

import scala.collection.mutable

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.scraping.Scraper.JobsScraped
import vn.johu.scraping.itviec.ItViecScraper
import vn.johu.scraping.jsoup.JSoup
import vn.johu.utils.Logging

class ScraperManager(context: ActorContext[ScraperManager.Command])
  extends AbstractBehavior[ScraperManager.Command] with Logging {

  import ScraperManager._

  private var scrapers = mutable.ListBuffer.empty[ActorRef[Scraper.Command]]

  private val jobsScrapedAdapter = context.messageAdapter[JobsScraped](WrappedJobsScraped.apply)

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case RunAllScrapers =>
        runAllScrapers()
        this
      case WrappedJobsScraped(jobsScraped) =>
        logger.debug(s"Scraped ${jobsScraped.scrapedJobs.size} jobs")
        this
    }
  }

  private def runAllScrapers(): Unit = {
    logger.info("Run all scrapers")

    if (scrapers.isEmpty) {
      logger.info("Found no scrapers. Creating them...")
      scrapers += context.spawn[Scraper.Command](ItViecScraper(JSoup), "ItViecScraper")
      logger.info(s"Created ${scrapers.size} scrapers: ${scrapers.map(_.path).mkString(",")}")
    }

    logger.info(s"Sending messages to ${scrapers.size} scrapers to start scraping...")

    scrapers.foreach { scraper =>
      scraper ! Scraper.Scrape(replyTo = jobsScrapedAdapter)
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

}
