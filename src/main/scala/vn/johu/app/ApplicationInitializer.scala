package vn.johu.app

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, PostStop, Signal}

import vn.johu.persistence.MongoDb
import vn.johu.utils.Logging

class ApplicationInitializer(context: ActorContext[ApplicationInitializer.Command])
  extends AbstractBehavior[ApplicationInitializer.Command] with Logging {

  import ApplicationInitializer._

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

object ApplicationInitializer {

  sealed trait Command

  case object InitSystem extends Command

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command](new ApplicationInitializer(_))
  }
}
