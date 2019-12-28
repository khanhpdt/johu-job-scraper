package vn.johu.scraping.scrapers

import akka.actor.typed.ActorRef

import vn.johu.scraping.models.ScrapedJob

object Scraper {

  sealed trait Command

  case class Scrape(page: Int = 1, replyTo: ActorRef[JobsScraped]) extends Command

  case class JobsScraped(page: Int, scrapedJobs: List[ScrapedJob])

  case class ParseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) extends Command

}
