package vn.johu.scraping

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}
import reactivemongo.api.bson.BSONDocument

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.utils.Logging

trait ScraperTestFixture extends FunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with Logging {

  protected val testKit: ActorTestKit = ActorTestKit("scraper-test-actor-system", ConfigFactory.load())

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
    val config = testKit.config
    MongoDb.init(config)
    RabbitMqClient.init(config)
  }

  override def beforeEach(): Unit = {
    deleteAllMongoDocs()
    RabbitMqClient.clearAll()
  }

  override def afterAll(): Unit = {
    MongoDb.close()
    RabbitMqClient.close()
    testKit.shutdownTestKit()
  }

}
