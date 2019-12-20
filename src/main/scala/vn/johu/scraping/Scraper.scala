package vn.johu.scraping

import akka.actor.typed.ActorRef

object Scraper {

  sealed trait Command

  case class StartScraping(replyTo: ActorRef[ScrapingCoordinator.Command]) extends Command

}
