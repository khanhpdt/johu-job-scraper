package vn.johu.scraping

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import reactivemongo.api.bson.BSONDocument

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.utils.Logging

trait ScraperTestFixture extends ScalaTestWithActorTestKit with FunSuiteLike with BeforeAndAfterAll with Logging {

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
    super.beforeAll()

    val config = system.settings.config
    MongoDb.init(config)
    RabbitMqClient.init(config)

    deleteAllMongoDocs()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    MongoDb.close()
    RabbitMqClient.close()
  }

}
