package vn.johu.app

import akka.actor.typed.ActorSystem

object ScraperApp extends App {
  val initializer = ActorSystem[AppInitializer.Command](AppInitializer(), "johu-actor-system")
  initializer ! AppInitializer.InitSystem
}
