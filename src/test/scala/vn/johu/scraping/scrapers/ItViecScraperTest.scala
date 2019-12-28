package vn.johu.scraping.scrapers

import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import org.scalatest.BeforeAndAfterEach

import Scraper.JobsScraped
import vn.johu.scraping.{HtmlMock, JSoupMock, ScraperTestFixture}

class ItViecScraperTest extends ScraperTestFixture with BeforeAndAfterEach {

  import testKit._

  // make this immutable for us to start/stop for each test method.
  // reason: our scraper needs to use html mocks so we need to refresh these htmls for each test.
  // if we don't restart the scraper, the test actor system will reuse the actor for multiple tests when we call spawn method.
  private var scraper: ActorRef[Scraper.Command] = _

  override def afterEach(): Unit = {
    super.afterEach()
    testKit.stop(scraper)
  }

  test("be able to parse jobs from html") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_raw_data/itviec/job_page_1.html")
    )

    scraper = spawn[Scraper.Command](ItViecScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    val response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs should have length 20
  }

  test("should continue scraping when all jobs were not scraped before") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_raw_data/itviec/job_page_1.html"),
      HtmlMock("https://itviec.com/it-jobs?page=2", "sample_raw_data/itviec/job_page_2.html")
    )

    scraper = spawn[Scraper.Command](ItViecScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)

    val response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs should have length 20

    probe.awaitAssert(
      {
        val response = probe.receiveMessage()
        response.page shouldBe 2
        response.scrapedJobs should have length 20
      },
      500.milliseconds
    )
  }

  test("should not continue scraping if all jobs are already scraped") {
    val htmlMocks = List(
      HtmlMock("https://itviec.com/it-jobs?page=1", "sample_raw_data/itviec/job_page_1.html"),
      HtmlMock("https://itviec.com/it-jobs?page=2", "sample_raw_data/itviec/job_page_1.html")
    )
    scraper = spawn[Scraper.Command](ItViecScraper(JSoupMock(htmlMocks)))
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.Scrape(replyTo = probe.ref)
    var response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs should have length 20

    response = probe.receiveMessage()
    response.page shouldBe 2
    response.scrapedJobs shouldBe empty

    // start another scraping
    scraper ! Scraper.Scrape(replyTo = probe.ref)
    response = probe.receiveMessage()
    response.page shouldBe 1
    response.scrapedJobs shouldBe empty

    // no scraping of page 2
    probe.expectNoMessage()
  }

}
