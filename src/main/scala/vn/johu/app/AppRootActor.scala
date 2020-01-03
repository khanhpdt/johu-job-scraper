package vn.johu.app

import scala.concurrent.ExecutionContext

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.http.{HttpClient, HttpServer}
import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scheduling.QuartzScheduler
import vn.johu.scraping.scrapers.JobScraperManager
import vn.johu.utils.Logging

class AppRootActor(context: ActorContext[AppRootActor.Command])
  extends AbstractBehavior[AppRootActor.Command] with Logging {

  import AppRootActor._

  private implicit val ec: ExecutionContext = context.system.executionContext

  private var scraperManager: ActorRef[JobScraperManager.Command] = _

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case InitSystem =>
        initSystem()
        this
      case RunAllScrapers =>
        scraperManager ! JobScraperManager.ScrapeAllSources
        this
    }
  }

  private def initSystem(): Unit = {
    logger.info("Initializing system...")

    val config = context.system.settings.config

    MongoDb.init(config)
    RabbitMqClient.init(config)
    QuartzScheduler.init(config)

    HttpClient.init(context.system)

    scraperManager = context.spawn(JobScraperManager(), "scraperManager")

    HttpServer.init(context.system, scraperManager)

    scraperManager ! JobScraperManager.Init
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("Application stopped. Closing resources...")
      MongoDb.close()
      RabbitMqClient.close()
      QuartzScheduler.close()
      HttpServer.close()
      this
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
