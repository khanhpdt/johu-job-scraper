package vn.johu.app

import akka.actor.typed.ActorSystem

object JobHubApplication extends App {
  val initializer = ActorSystem[ApplicationInitializer.Command](ApplicationInitializer(), "johu-actor-system")
  initializer ! ApplicationInitializer.InitSystem
}
