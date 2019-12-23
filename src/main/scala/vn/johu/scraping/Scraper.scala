package vn.johu.scraping

import akka.actor.typed.ActorRef

object Scraper {

  sealed trait Command

  case class Scrape(page: Int = 1, replyTo: ActorRef[ScrapingCoordinator.JobsScraped]) extends Command

}
