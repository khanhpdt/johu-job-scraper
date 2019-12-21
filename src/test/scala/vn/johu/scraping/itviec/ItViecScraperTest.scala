package vn.johu.scraping.itviec

import scala.io.Source

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.FunSuiteLike

import vn.johu.scraping.Scraper
import vn.johu.scraping.ScrapingCoordinator.JobsScraped
import vn.johu.scraping.jsoup.HtmlDoc

class ItViecScraperTest extends ScalaTestWithActorTestKit with FunSuiteLike {

  test("be able to parse jobs from html") {
    val scraper = spawn[Scraper.Command](ItViecScraper())
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.ParseDoc(
      HtmlDoc.fromHtml(Source.fromResource("sample_htmls/itviec/job_page_1.html").mkString),
      probe.ref
    )

    val jobs = probe.receiveMessage().scrapedJobs
    jobs should have length 19
  }

}
