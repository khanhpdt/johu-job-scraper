package vn.johu.app

import scala.collection.mutable
import scala.concurrent.ExecutionContext

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.http.HttpServer
import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scheduling.QuartzScheduler
import vn.johu.scraping.Scraper
import vn.johu.scraping.Scraper.JobsScraped
import vn.johu.scraping.itviec.ItViecScraper
import vn.johu.scraping.jsoup.JSoup
import vn.johu.utils.Logging

class AppRootActor(context: ActorContext[AppRootActor.Command])
  extends AbstractBehavior[AppRootActor.Command] with Logging {

  import AppRootActor._

  private implicit val ec: ExecutionContext = context.system.executionContext

  private var scrapers = mutable.ListBuffer.empty[ActorRef[Scraper.Command]]

  private val jobsScrapedAdapter = context.messageAdapter[JobsScraped](WrappedJobsScraped.apply)

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case InitSystem =>
        initSystem()
        this
      case RunAllScrapers =>
        runAllScrapers()
        this
      case WrappedJobsScraped(jobsScraped) =>
        logger.debug(s"Scraped ${jobsScraped.scrapedJobs.size} jobs")
        this
    }
  }

  private def initSystem(): Unit = {
    logger.info("Initializing system...")
    val config = context.system.settings.config
    MongoDb.init(config)
    RabbitMqClient.init(config)
    QuartzScheduler.init(config)
    HttpServer.init(context.system)
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("Application stopped. Closing resources...")
      MongoDb.close()
      RabbitMqClient.close()
      QuartzScheduler.close()
      HttpServer.close()
      this
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
}

object AppRootActor {

  sealed trait Command

  case object InitSystem extends Command

  case object RunAllScrapers extends Command

  case class WrappedJobsScraped(jobsScraped: JobsScraped) extends Command

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command](new AppRootActor(_))
  }
}
