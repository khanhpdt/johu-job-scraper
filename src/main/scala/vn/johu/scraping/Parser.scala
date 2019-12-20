package vn.johu.scraping

import akka.actor.typed.ActorRef

object Parser {

  sealed trait Command

  case class StartParsing(replyTo: ActorRef[ScrapingCoordinator.Command]) extends Command

}
