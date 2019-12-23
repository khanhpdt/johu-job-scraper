package vn.johu.scraping.itviec

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success}

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import reactivemongo.api.bson.BSONDocument

import vn.johu.persistence.MongoDb
import vn.johu.scraping.Scraper
import vn.johu.scraping.ScrapingCoordinator.JobsScraped
import vn.johu.scraping.jsoup.HtmlDoc
import vn.johu.utils.Logging

class ItViecScraperTest extends ScalaTestWithActorTestKit with FunSuiteLike with BeforeAndAfterAll with Logging {

  private def deleteAllMongoDocs(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val deleteF = Future.sequence {
      MongoDb.allCollections.map { coll =>
        coll.flatMap(_.delete(ordered = false).one(BSONDocument.empty))
      }
    }

    deleteF.andThen {
      case Failure(exception) => throw exception
      case Success(_) => logger.info("All Mongo docs deleted.")
    }

    Await.ready(deleteF, 1.minutes)
  }

  override def beforeAll(): Unit = {
    MongoDb.init(system.settings.config)
    deleteAllMongoDocs()
  }

  override def afterAll(): Unit = {
    MongoDb.close()
  }

  test("be able to parse jobs from html") {
    val scraper = spawn[Scraper.Command](ItViecScraper())
    val probe = createTestProbe[JobsScraped]()

    scraper ! Scraper.ParseDoc(
      HtmlDoc.fromHtml(Source.fromResource("sample_htmls/itviec/job_page_1.html").mkString),
      probe.ref
    )

    val jobs = probe.receiveMessage().scrapedJobs
    jobs should have length 20
  }

}
