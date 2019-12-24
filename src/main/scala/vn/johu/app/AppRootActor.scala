package vn.johu.app

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{Behavior, PostStop, Signal}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scheduling.QuartzScheduler
import vn.johu.utils.Logging

class AppRootActor(context: ActorContext[AppRootActor.Command])
  extends AbstractBehavior[AppRootActor.Command] with Logging {

  import AppRootActor._

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case InitSystem =>
        initSystem()
        this
      case RunAllScrapers =>
        runAllScrapers()
        this
    }
  }

  private def initSystem(): Unit = {
    logger.info("Initializing system...")
    val config = context.system.settings.config
    MongoDb.init(config)
    RabbitMqClient.init(config)
    QuartzScheduler.init()
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("Application stopped.")
      MongoDb.close()
      RabbitMqClient.close()
      QuartzScheduler.close()
      this
  }

  private def runAllScrapers(): Unit = {
    logger.info("Run all scrapers")
  }
}

object AppRootActor {

  sealed trait Command

  case object InitSystem extends Command

  case object RunAllScrapers extends Command

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command](new AppRootActor(_))
  }
}
