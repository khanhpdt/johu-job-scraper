package vn.johu.app

import akka.actor.typed.{ActorRef, ActorSystem}

object ScraperApp  {

  var rootActor: ActorRef[AppRootActor.Command] = _

  def main(args: Array[String]): Unit = {
    rootActor = ActorSystem[AppRootActor.Command](AppRootActor(), "johu-actor-system")
    rootActor ! AppRootActor.InitSystem
  }

  def runScrapers(): Unit = {
    rootActor ! AppRootActor.RunAllScrapers
  }

}
