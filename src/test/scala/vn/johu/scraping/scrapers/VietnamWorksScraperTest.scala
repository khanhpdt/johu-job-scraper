package vn.johu.scraping.scrapers

import vn.johu.scraping.scrapers.Scraper.JobsScraped
import vn.johu.scraping.{HttpClientMock, HttpResponseMock, ScraperTestFixture}

class VietnamWorksScraperTest extends ScraperTestFixture {

  import testKit._

  test("be able to parse jobs from json") {
    val responseMocks = List(
      HttpResponseMock(jsonFilePath = "sample_raw_data/vietnamworks/job_page_0.json")
    )

    scraper = spawn[Scraper.Command](VietnamWorksScraper(HttpClientMock(responseMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(page = 0, replyTo = probe.ref)

    val response = probe.receiveMessage()
    response.page shouldBe 0
    response.scrapedJobs should have length 50
  }

}
