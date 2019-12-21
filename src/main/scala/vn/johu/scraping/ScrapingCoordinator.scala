package vn.johu.scraping

import scala.concurrent.ExecutionContext

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.persistence.JobRepository
import vn.johu.scraping.models.{ParsedJob, ScrapedJob}
import vn.johu.utils.Logging

class ScrapingCoordinator(
  context: ActorContext[ScrapingCoordinator.Command],
  scraper: ActorRef[Scraper.Command],
  parser: ActorRef[Parser.Command],
  repository: ActorRef[JobRepository.Command]
) extends AbstractBehavior[ScrapingCoordinator.Command] with Logging {

  import ScrapingCoordinator._

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case StartScraping =>
        logger.info(s"Start coordinating scraping from company...")
        scraper ! Scraper.StartScraping(context.self)
        this
      case JobsScraped(scrapedJobs) =>
        logger.info(s"Scraped ${scrapedJobs.size} pages. Saving those pages...")
        repository ! JobRepository.SaveScrapedJobs(scrapedJobs, context.self)
        this
      case ScrapedJobsSaved | StartParsing =>
        logger.info("Parsing scraped jobs...")
        parser ! Parser.StartParsing(context.self)
        this
      case JobsParsed(parsedJobs) =>
        logger.info(s"Parsed ${parsedJobs.size} jobs. Saving those jobs...")
        repository ! JobRepository.SaveParsedJobs(parsedJobs, context.self)
        this
      case ParsedJobsSaved(savedJobs) =>
        logger.info(s"Saved ${savedJobs.size} parsed jobs. Publishing those jobs...")
        repository ! JobRepository.PublishJob(savedJobs, context.self)
        this
      case ParsedJobsPublished =>
        logger.info("Jobs published.")
        this
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("ScrapingCoordinator stopped.")
      this
  }

}

object ScrapingCoordinator {

  sealed trait Command

  case object StartScraping extends Command

  case class JobsScraped(scrapedJobs: List[ScrapedJob]) extends Command

  case object ScrapedJobsSaved extends Command

  case object StartParsing extends Command

  case class JobsParsed(jobs: List[ParsedJob]) extends Command

  case class ParsedJobsSaved(savedJobs: List[ParsedJob]) extends Command

  case object ParsedJobsPublished extends Command

}
