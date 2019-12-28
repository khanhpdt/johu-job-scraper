package vn.johu.scraping

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}
import reactivemongo.api.bson.BSONDocument

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scraping.scrapers.Scraper
import vn.johu.utils.Logging

trait ScraperTestFixture extends FunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with Logging {

  protected val testKit: ActorTestKit = ActorTestKit("scraper-test-actor-system", ConfigFactory.load())

  private implicit val ec: ExecutionContext = testKit.system.executionContext

  // make this mutable for us to start/stop for each test method.
  // reason: our scraper needs to use data mocks so we need to refresh these data for each test.
  // if we don't restart the scraper, the test actor system will reuse the actor for multiple tests when we call spawn method.
  // TODO: remove this var and the stopping of this scraper in afterEach() if possible
  protected var scraper: ActorRef[Scraper.Command] = _

  private def deleteAllMongoDocs(): Unit = {
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
    val config = testKit.config
    MongoDb.init(config)
    RabbitMqClient.init(config)
  }

  override def beforeEach(): Unit = {
    deleteAllMongoDocs()
    RabbitMqClient.clearAll()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    testKit.stop(scraper)
  }

  override def afterAll(): Unit = {
    MongoDb.close()
    RabbitMqClient.close()
    testKit.shutdownTestKit()
  }

}
