package vn.johu.scraping.scrapers

import scala.concurrent.Await
import scala.concurrent.duration._

import vn.johu.persistence.DocRepo
import vn.johu.scraping.models.RawJobSourceName
import vn.johu.scraping.scrapers.Scraper.ScrapePagesResult
import vn.johu.scraping.{HttpClientMock, HttpResponseMock, ScraperTestFixture}

class VietnamWorksJobScraperTest extends ScraperTestFixture {

  import testKit._

  test("be able to parse jobs from json") {
    val responseMocks = List(
      HttpResponseMock(jsonFilePath = "sample_raw_data/vietnamworks/job_page_0.json")
    )

    scraper = spawn[Scraper.Command](VietnamWorksJobScraper(HttpClientMock(responseMocks)))
    val probe = createTestProbe[ScrapePagesResult]()

    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(1))

    val response = probe.receiveMessage()
    response.startPage shouldBe 1
    response.newJobs should have length 50
    response.existingJobs should have length 0
  }

  test("parse job details correctly") {
    val responseMocks = List(
      HttpResponseMock(jsonFilePath = "sample_raw_data/vietnamworks/one_job_page.json")
    )

    scraper = spawn[Scraper.Command](VietnamWorksJobScraper(HttpClientMock(responseMocks)))
    val probe = createTestProbe[ScrapePagesResult]()

    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(1))

    val response = probe.receiveMessage()
    response.startPage shouldBe 1
    response.newJobs should have length 1
    response.existingJobs should have length 0

    val job = response.newJobs.head
    job.id shouldBe defined
    job.url shouldBe s"${VietnamWorksJobScraper.BaseUrl}/application-support-specialist-3-1199830-jv"
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

    scraper = spawn[Scraper.Command](VietnamWorksJobScraper(HttpClientMock(responseMocks)))
    val probe = createTestProbe[ScrapePagesResult]()

    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(1))

    val existingJob = probe.awaitAssert {
      val jobs = Await.result(DocRepo.findAllJobs(), 100.milliseconds)
      jobs should have length 1
      jobs.head
    }

    // next scraping
    scraper ! Scraper.ScrapePages(replyTo = probe.ref, endPage = Some(1))

    probe.awaitAssert {
      val jobs = Await.result(DocRepo.findAllJobs(), 100.milliseconds)
      jobs should have length 1
      jobs.head.id shouldBe existingJob.id
      jobs.head.modifiedTs.get.value should be > existingJob.modifiedTs.get.value
    }
  }

}
