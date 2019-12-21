package vn.johu.scraping

import akka.actor.typed.ActorRef

import vn.johu.scraping.jsoup.HtmlDoc

object Scraper {

  sealed trait Command

  case class StartScraping(replyTo: ActorRef[ScrapingCoordinator.JobsScraped]) extends Command

  case class ParseDoc(doc: HtmlDoc, replyTo: ActorRef[ScrapingCoordinator.JobsScraped]) extends Command

}
