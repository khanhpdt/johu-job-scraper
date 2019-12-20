package vn.johu.persistence

import akka.actor.typed.ActorRef

import vn.johu.scraping.ScrapingCoordinator
import vn.johu.scraping.models.{ParsedJob, ScrapedJob}

class JobRepository {

}

object JobRepository {

  sealed trait Command

  case class PublishJob(jobs: List[ParsedJob], replyTo: ActorRef[ScrapingCoordinator.Command]) extends Command

  case class SaveScrapedJobs(jobs: List[ScrapedJob], replyTo: ActorRef[ScrapingCoordinator.Command]) extends Command

  case class SaveParsedJobs(jobs: List[ParsedJob], replyTo: ActorRef[ScrapingCoordinator.Command]) extends Command

}
