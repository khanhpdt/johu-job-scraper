package vn.johu.scraping.itviec

import scala.concurrent.duration._

import vn.johu.scraping.ScrapingCoordinator.JobsScraped
import vn.johu.scraping.{HtmlMock, JSoupMock, Scraper, ScraperTestFixture}

class ItViecScraperTest extends ScraperTestFixture {

  test("be able to parse jobs from html") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_htmls/itviec/job_page_1.html")
    )

    val scraper = spawn[Scraper.Command](ItViecScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    val jobs = probe.receiveMessage().scrapedJobs
    jobs should have length 20
  }

  test("should continue scraping when all jobs were not scraped before") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_htmls/itviec/job_page_1.html"),
      HtmlMock("https://itviec.com/it-jobs?page=2", "sample_htmls/itviec/job_page_2.html")
    )
    
    val scraper = spawn[Scraper.Command](ItViecScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    val jobs = probe.receiveMessage().scrapedJobs
    jobs should have length 20

    probe.awaitAssert(
      {
        val jobs = probe.receiveMessage().scrapedJobs
        jobs should have length 20
      },
      500.milliseconds
    )
  }

}
