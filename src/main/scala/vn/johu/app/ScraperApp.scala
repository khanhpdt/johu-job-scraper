package vn.johu.app

import akka.actor.typed.ActorSystem

object ScraperApp extends App {

  val rootActor = ActorSystem[AppRootActor.Command](AppRootActor(), "johu-actor-system")
  rootActor ! AppRootActor.InitSystem

  def runScrapers(): Unit = {
    rootActor ! AppRootActor.RunAllScrapers
  }

}
