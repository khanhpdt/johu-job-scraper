package vn.johu.app

import akka.actor.typed.ActorSystem

import vn.johu.utils.Logging

object ScraperApp extends App with Logging {

  val rootActor = ActorSystem[AppRootActor.Command](AppRootActor(), "johu-actor-system")

  sys.addShutdownHook {
    logger.info("Shutting down the application...")

    // TODO: test this again. it seems it just hangs.
    rootActor.terminate()
  }

  rootActor ! AppRootActor.InitSystem

  def runScrapers(): Unit = {
    rootActor ! AppRootActor.RunAllScrapers
  }

}
