package vn.johu.scraping.scrapers

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.scraping.jsoup.JSoup
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models.{RawJobSourceName, ScrapedJob}
import vn.johu.scraping.scrapers.JobDetailsScraper.ScrapeJobDetailsResult
import vn.johu.scraping.scrapers.Scraper.ScrapePagesResult
import vn.johu.utils.{Configs, Logging}

class JobScraperManager(
  context: ActorContext[JobScraperManager.Command],
  timer: TimerScheduler[JobScraperManager.Command]
) extends AbstractBehavior[JobScraperManager.Command] with Logging {

  import JobScraperManager._

  private var jobScrapers = mutable.Map.empty[RawJobSourceName, ActorRef[Scraper.Command]]

  private var jobDetailsScrapers = mutable.Map.empty[RawJobSourceName, ActorRef[JobDetailsScraper.Command]]

  private val scrapePagesResultAdapter = context.messageAdapter[ScrapePagesResult](ScrapeFromSourcesResult.apply)

  private val scrapeJobDetailsResultAdapter = context.messageAdapter[ScrapeJobDetailsResult](WrappedScrapeJobDetailsResult.apply)

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Init =>
        init()
        this
      case ScrapeAllSources =>
        scrapeFromAllSources()
        this
      case ParseRawJobSources(rawJobSourceName, start, end) =>
        parseLocalJobSources(rawJobSourceName, start, end)
        this
      case ScrapeFromSources(jobSources, endPage) =>
        scrapeFromSources(jobSources, endPage)
        this
      case ScrapeFromSourcesResult(result) =>
        scheduleScrapingJobDetails(result)
        this
      case ScrapeJobDetails(rawJobSourceName, jobs) =>
        scrapeJobDetails(rawJobSourceName, jobs)
        this
      case _ =>
        Behaviors.unhandled
    }
  }

  private def init(): Unit = {
    addAllScrapers()
    logger.info("ScraperManager initialized.")
  }

  private def addAllScrapers(): Unit = {
    def addJobScraper(jobSource: RawJobSourceName): Unit = {
      val newScraper = jobSource match {
        case RawJobSourceName.ItViec =>
          context.spawn[Scraper.Command](ItViecJobScraper(JSoup), "ItViecScraper")
        case RawJobSourceName.VietnamWorks =>
          context.spawn[Scraper.Command](VietnamWorksJobScraper(), "VietnamWorksScraper")
        case _ =>
          throw new IllegalArgumentException(s"Scraper for source $jobSource not supported yet.")
      }

      jobScrapers += jobSource -> newScraper

      logger.info(s"Job scraper for $jobSource added.")
    }

    def addJobDetailsScraper(jobSource: RawJobSourceName): Unit = {
      val newScraper = jobSource match {
        case RawJobSourceName.ItViec =>
          context.spawn[JobDetailsScraper.Command](JobDetailsScraper(JSoup, jobSource), "ItViecJobDetailsScraper")
        case RawJobSourceName.VietnamWorks =>
          context.spawn[JobDetailsScraper.Command](JobDetailsScraper(JSoup, jobSource), "VietnamWorksScraper")
        case _ =>
          throw new IllegalArgumentException(s"Job details Scraper for source $jobSource not supported yet.")
      }

      jobDetailsScrapers += jobSource -> newScraper

      logger.info(s"Job details scraper for $jobSource added.")
    }

    Set(RawJobSourceName.ItViec, RawJobSourceName.VietnamWorks).foreach { source =>
      addJobScraper(source)
      addJobDetailsScraper(source)
    }
  }

  private def scrapeFromSources(jobSources: List[RawJobSourceName.RawJobSourceName], endPage: Option[Int]): Unit = {
    if (jobSources.isEmpty) {
      logger.info("No sources passed. Run all scrapers.")
      scrapeFromAllSources()
    } else {
      logger.info(s"Running scrapers from sources: ${jobSources.mkString(",")}")
      jobSources.foreach { source =>
        getJobScraper(source) ! Scraper.ScrapePages(endPage = endPage, replyTo = scrapePagesResultAdapter)
      }
    }
  }

  private def parseLocalJobSources(jobSource: RawJobSourceName, startTs: Option[String], endTs: Option[String]): Unit = {
    logger.info(s"Start parsing from local job source: $jobSource...")
    getJobScraper(jobSource) ! Scraper.ParseLocalRawJobSources(startTs, endTs)
  }

  private def getJobScraper(jobSource: RawJobSourceName) = {
    jobScrapers.getOrElse(jobSource, throw new IllegalArgumentException(s"Scraper for source $jobSource not supported yet."))
  }

  private def scrapeFromAllSources(): Unit = {
    logger.info(s"Running ${jobScrapers.size} scrapers: ${jobScrapers.keys.mkString(", ")}")
    jobScrapers.foreach { scraper =>
      scraper._2 ! Scraper.ScrapePages(replyTo = scrapePagesResultAdapter)
    }
  }

  private def scheduleScrapingJobDetails(result: ScrapePagesResult): Unit = {
    val config = context.system.settings.config
    val delay = FiniteDuration(config.getLong(Configs.ScrapingJobDetailsAfterScrapingJobsDelayInSeconds), TimeUnit.SECONDS)

    timer.startSingleTimer(
      getTimerKey(result.rawJobSourceName),
      ScrapeJobDetails(result.rawJobSourceName, result.newJobs),
      delay = delay
    )

    logger.info(
      s"Will scrape job details for ${result.newJobs.size} new jobs " +
        s"from ${result.rawJobSourceName} in ${delay.toMinutes} minutes."
    )
  }

  def getTimerKey(rawJobSourceName: RawJobSourceName): Any = {
    rawJobSourceName match {
      case RawJobSourceName.ItViec =>
        ItViecJobDetailsScrapingTimerKey
      case RawJobSourceName.VietnamWorks =>
        VietnamWorksJobDetailsScrapingTimerKey
      case _ =>
        throw new IllegalArgumentException(s"Not supporting $rawJobSourceName yet.")
    }
  }

  private def scrapeJobDetails(rawJobSourceName: RawJobSourceName, jobs: List[ScrapedJob]): Unit = {
    getJobDetailsScraper(rawJobSourceName) ! JobDetailsScraper.ScrapeJobDetails(jobs, scrapeJobDetailsResultAdapter)
  }

  private def getJobDetailsScraper(rawJobSourceName: RawJobSourceName): ActorRef[JobDetailsScraper.Command] = {
    jobDetailsScrapers.getOrElse(
      rawJobSourceName,
      throw new IllegalArgumentException(s"No job details scraper defined for source $rawJobSourceName")
    )
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("ScraperManager stopped.")
      this
  }
}

object JobScraperManager {

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command] { ctx =>
      Behaviors.withTimers { timer =>
        new JobScraperManager(ctx, timer)
      }
    }
  }

  sealed trait Command

  case object Init extends Command

  case object ScrapeAllSources extends Command

  case class ScrapeFromSources(jobSources: List[RawJobSourceName], endPage: Option[Int] = None) extends Command

  case class ScrapeFromSourcesResult(scrapeResult: ScrapePagesResult) extends Command

  case class ParseRawJobSources(
    jobSourceName: RawJobSourceName,
    scrapingStartTs: Option[String],
    scrapingEndTs: Option[String]
  ) extends Command

  case class ScrapeJobDetails(jobSourceName: RawJobSourceName, jobs: List[ScrapedJob]) extends Command

  private case class WrappedScrapeJobDetailsResult(result: ScrapeJobDetailsResult) extends Command

  private case object ItViecJobDetailsScrapingTimerKey

  private case object VietnamWorksJobDetailsScrapingTimerKey

}
