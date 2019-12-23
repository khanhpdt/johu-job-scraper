package vn.johu.app

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, PostStop, Signal}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.utils.Logging

class AppInitializer(context: ActorContext[AppInitializer.Command])
  extends AbstractBehavior[AppInitializer.Command] with Logging {

  import AppInitializer._

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case InitSystem =>
        logger.info("Initializing system...")
        val config = context.system.settings.config
        MongoDb.init(config)
        RabbitMqClient.init(config)
        Behaviors.stopped
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("Application stopped.")
      MongoDb.close()
      RabbitMqClient.close()
      this
  }
}

object AppInitializer {

  sealed trait Command

  case object InitSystem extends Command

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command](new AppInitializer(_))
  }
}
