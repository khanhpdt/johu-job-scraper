package vn.johu.scraping.scrapers

import vn.johu.scraping.models.RawJobSourceName
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

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    val response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs should have length 50
  }

  test("parse job details correctly") {
    val responseMocks = List(
      HttpResponseMock(jsonFilePath = "sample_raw_data/vietnamworks/one_job_page.json")
    )

    scraper = spawn[Scraper.Command](VietnamWorksScraper(HttpClientMock(responseMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    val response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs should have length 1

    val job = response.scrapedJobs.head
    job.id shouldBe defined
    job.url shouldBe s"${VietnamWorksScraper.BaseUrl}/application-support-specialist-3-1199830-jv"
    job.title shouldBe "Application Support Specialist"
    job.tags should contain theSameElementsAs Set("End-User Support", "It Helpdesk", "Technical Support", "System Support", "It Support")
    job.postingDate.toLong.get shouldBe 1577530825000L
    job.company shouldBe "Home Credit Vietnam"
    job.locations should contain theSameElementsAs Set("Ho Chi Minh")
    job.rawJobSourceName shouldBe RawJobSourceName.VietnamWorks
  }

  test("be able to update job") {
    val responseMocks = List(
      HttpResponseMock(jsonFilePath = "sample_raw_data/vietnamworks/one_job_page.json")
    )

    scraper = spawn[Scraper.Command](VietnamWorksScraper(HttpClientMock(responseMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    var response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs should have length 1

    // next scraping
    scraper ! Scraper.Scrape(replyTo = probe.ref)

    // todo: assert modifiedTs changed
  }

}
