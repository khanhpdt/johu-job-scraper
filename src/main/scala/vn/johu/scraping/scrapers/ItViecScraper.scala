package vn.johu.scraping.scrapers

import scala.concurrent.Future

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}

import vn.johu.scraping.jsoup.JSoup
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models._

class ItViecScraper(
  context: ActorContext[Scraper.Command],
  jSoup: JSoup,
  timer: TimerScheduler[Scraper.Command]
) extends Scraper(context, timer) {

  import ItViecScraper._

  override protected def getRawJobSourceContent(page: Int): Future[String] = {
    val pageUrl = s"$BaseUrl/it-jobs?page=$page"
    jSoup.getAsync(pageUrl).map(_.doc.outerHtml())
  }

  override protected def parseJobsFromRaw(rawJobSource: RawJobSource): JobParsingResult = {
    ItViecParser.parseJobs(rawJobSource)
  }

  override protected val rawJobSourceName: RawJobSourceName = RawJobSourceName.ItViec

  override protected val timerKey: Any = TimerKey

}

object ItViecScraper {

  val BaseUrl = "https://itviec.com"

  def apply(jSoup: JSoup): Behavior[Scraper.Command] = {
    Behaviors.setup[Scraper.Command] { ctx =>
      Behaviors.withTimers[Scraper.Command] { timers =>
        new ItViecScraper(ctx, jSoup, timers)
      }
    }
  }

  case object TimerKey

}