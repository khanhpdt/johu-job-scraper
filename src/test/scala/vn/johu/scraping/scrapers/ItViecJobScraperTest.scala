package vn.johu.scraping.scrapers

import scala.concurrent.duration._

import vn.johu.scraping.scrapers.Scraper.ScrapePagesResult
import vn.johu.scraping.{HtmlMock, JSoupMock, ScraperTestFixture}

class ItViecJobScraperTest extends ScraperTestFixture {

  import testKit._

  test("be able to parse jobs from html") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_raw_data/itviec/job_page_1.html")
    )

    scraper = spawn[Scraper.Command](ItViecJobScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[ScrapePagesResult]()

    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(1))

    val response = probe.receiveMessage()
    response.startPage shouldBe 1
    response.newJobs should have length 20
    response.existingJobs should have length 0
  }

  test("should continue scraping when all jobs in page 1 were not scraped before") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_raw_data/itviec/job_page_1.html"),
      HtmlMock("https://itviec.com/it-jobs?page=2", "sample_raw_data/itviec/job_page_2.html")
    )

    scraper = spawn[Scraper.Command](ItViecJobScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[ScrapePagesResult]()

    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(2))

    val response = probe.receiveMessage()
    response.startPage shouldBe 1
    response.newJobs should have length 40
  }

  test("should not continue scraping if all jobs in page 1 were already scraped") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_raw_data/itviec/job_page_1.html"),
      HtmlMock("https://itviec.com/it-jobs?page=2", "sample_raw_data/itviec/job_page_2.html")
    )
    scraper = spawn[Scraper.Command](ItViecJobScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[ScrapePagesResult]()

    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(2))
    var response = probe.receiveMessage()
    response.startPage shouldBe 1
    response.newJobs should have length 40
    response.existingJobs should have length 0

    // start another scraping
    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(2))
    response = probe.receiveMessage()
    response.startPage shouldBe 1
    response.newJobs shouldBe empty
    // it stopped at page 1 when if found out that all jobs in page 1 are already scraped
    response.existingJobs should have length 20

    // no scraping of any next page
    probe.expectNoMessage()
  }

}
