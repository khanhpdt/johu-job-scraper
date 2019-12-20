package vn.johu.scraping

import scala.concurrent.duration._

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}

import vn.johu.utils.{Configs, Logging}

class ScraperScheduler(
  context: ActorContext[ScraperScheduler.Command],
  timers: TimerScheduler[ScraperScheduler.Command]
) extends AbstractBehavior[ScraperScheduler.Command]
  with Logging {

  import ScraperScheduler._

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Start =>
        timers.startPeriodicTimer(RunScrapers, RunScrapers, 12.hours)
        this
      case RunScrapers =>
        runScrapers()
        this
    }
  }

  private def runScrapers(): Unit = {
    val config = context.system.settings.config
    if (!config.getBoolean(Configs.ScrapingEnabled)) {
      logger.info("Scraping is disabled. No scraper will be scheduled.")
    } else {
      logger.info("Running scrapers...")

      def run[T](scraper: ActorRef[T], enabled: Boolean): Unit = {
        if (!enabled) {
          logger.info(s"Scraper [${scraper.path}] disabled.")
        } else {
          logger.info(s"Running scraper [${scraper.path}]...")
        }
      }
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      logger.info("ScraperScheduler stopped. Cancelling all timers.")
      timers.cancelAll()
      this
  }

}

object ScraperScheduler {

  def apply(): Behavior[Command] = {
    Behaviors.setup[Command] { ctx =>
      Behaviors.withTimers[Command] { timers =>
        new ScraperScheduler(ctx, timers)
      }
    }
  }

  sealed trait Command

  case object Start extends Command

  case object RunScrapers extends Command

}

case class ScheduleConfig(initialDelay: FiniteDuration = 500.milliseconds, interval: FiniteDuration)