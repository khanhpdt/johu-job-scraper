package vn.johu.app

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, PostStop, Signal}

import vn.johu.persistence.MongoDb
import vn.johu.utils.Logging

class AppInitializer(context: ActorContext[AppInitializer.Command])
  extends AbstractBehavior[AppInitializer.Command] with Logging {

  import AppInitializer._

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case InitSystem =>
        logger.info("Initializing system...")
        MongoDb.init(context.system.settings.config)
        Behaviors.stopped
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("Initialization done. ApplicationInitilizer stopped")
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
