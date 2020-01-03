package vn.johu.scraping.scrapers

import scala.concurrent.Future

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}

import vn.johu.http.HttpClient
import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models.{RawJobSource, RawJobSourceName}

class VietnamWorksJobScraper(
  context: ActorContext[Scraper.Command],
  timers: TimerScheduler[Scraper.Command],
  httpClient: HttpClient
) extends Scraper(context, timers) {

  import VietnamWorksJobScraper._

  override protected val rawJobSourceName: RawJobSourceName = RawJobSourceName.VietnamWorks

  override protected def getRawJobSourceContent(page: Int): Future[String] = {
    // page in Vietnamworks starts from 0
    val body =
      s"""
         |{
         |    "requests": [
         |        {
         |            "indexName": "vnw_job_v2_35",
         |            "params": "query=&hitsPerPage=50&page=${page - 1}&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"
         |        }
         |    ]
         |}
         |""".stripMargin

    httpClient.post(RawSourceUrl, body.getBytes).map(_.toString)
  }

  override protected def parseJobsFromRaw(rawJobSource: RawJobSource): JobParsingResult = {
    VietnamWorksJobParser.parseJobs(rawJobSource)
  }

  override protected val timerKey: Any = TimerKey

}

object VietnamWorksJobScraper {

  val BaseUrl = "https://www.vietnamworks.com"

  private val RawSourceUrl = s"https://jf8q26wwud-dsn.algolia.net/1/indexes/*/queries" +
    s"?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%20(lite)%203.24.5%3Binstantsearch.js%201.6.0%3BJS%20Helper%202.21.2" +
    s"&x-algolia-application-id=JF8Q26WWUD" +
    s"&x-algolia-api-key=NGJhZDA0N2ZjNDZmNTgxYzVlMzBiZTQxODVmODRiYWQwYWJiODQ1N2VhNGE4NTBhZmJiYzE3NTQ1ZTVkMWM0OHRhZ0ZpbHRlcnM9JnVzZXJUb2tlbj0zOWZlMzYyYTZmZmFjYzQ1ZWM2ZmRjOGRhMTMyNGFjMw%3D%3D"

  def apply(httpClient: HttpClient = HttpClient): Behavior[Scraper.Command] = {
    Behaviors.setup[Scraper.Command] { ctx =>
      Behaviors.withTimers[Scraper.Command] { timer =>
        new VietnamWorksJobScraper(ctx, timer, httpClient)
      }
    }
  }

  case object TimerKey

}
