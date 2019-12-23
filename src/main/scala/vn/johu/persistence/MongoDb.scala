package vn.johu.persistence

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.api.{AsyncDriver, DefaultDB, MongoConnection}

import vn.johu.utils.{Configs, Logging, TryHelper}

object MongoDb extends TryHelper with Logging {

  // TODO: Replace this with another context if needed

  import scala.concurrent.ExecutionContext.Implicits.global

  private val ScrapedJobCollectionName = "scraped_job"
  private val RawJobSourceCollectionName = "raw_job_source"
  private val JobParsingErrorCollectionName = "job_parsing_error"

  private var dbName: String = _
  private var mongoConnection: MongoConnection = _

  def init(config: Config): Unit = {
    logger.info("Initializing MongoDb database...")

    val hostUrl = config.getString(Configs.MongoHostUrl)
    dbName = config.getString(Configs.MongoDbName)
    val mongoUri = s"mongodb://$hostUrl/$dbName"
    val parsedUri = getTryResult(
      MongoConnection.parseURI(mongoUri),
      logger.error(s"Could not parse mongo URI: $mongoUri", _)
    )

    val driver = AsyncDriver()
    mongoConnection = getTryResult(
      Try(Await.result(driver.connect(parsedUri), 3.minutes)),
      logger.error(s"Could not connect to MongoDb using URI: $parsedUri", _)
    )

    checkDb()
  }

  def close(): Unit = {
    logger.info("Closing MongoDb connection...")
    mongoConnection.close()(5.minutes).onComplete {
      case Failure(ex) =>
        logger.error("Error when closing MongoDb connection.", ex)
      case Success(_) =>
        logger.info("MongoDb connection closed.")
    }
  }

  /*
   * From reactivemongo doc:
   * It’s generally a good practice not to assign the database and collection references to val (even to lazy val),
   * as it’s better to get a fresh reference each time, to automatically recover from any previous issues (e.g. network failure).
   */
  def database: Future[DefaultDB] = mongoConnection.database(dbName)

  def scrapedJobColl: Future[BSONCollection] = collection(ScrapedJobCollectionName)

  def rawJobSourceColl: Future[BSONCollection] = collection(RawJobSourceCollectionName)

  def jobParsingErrorColl: Future[BSONCollection] = collection(JobParsingErrorCollectionName)

  def allCollections = List(scrapedJobColl, rawJobSourceColl, jobParsingErrorColl)

  private def collection(name: String) = database.map(_.collection[BSONCollection](name))

  private def checkDb(): Unit = {
    logger.info("Checking database collections...")

    def checkCollection(collectionsInDb: Set[String], coll: CollectionConfig) = {
      val createCollF =
        if (!collectionsInDb.contains(coll.name)) {
          logger.info(s"Collection [${coll.name}] not exist. Creating...")
          collection(coll.name).flatMap(_.create())
        } else {
          logger.info(s"Found collection [${coll.name}].")
          Future.successful(())
        }

      logger.info("Checking indexes on collection...")
      for {
        _ <- createCollF
        dbColl <- collection(coll.name)
        dbIndices <- dbColl.indexesManager.list()
      } yield {
        val dbIndexNames = dbIndices.flatMap(_.name).toSet
        coll.indices.filter(idx => !dbIndexNames.contains(idx.name)).foreach { notExistingIndex =>
          logger.info(s"Index ${notExistingIndex.name} not exist on collection ${coll.name}. Creating...")
          dbColl.indexesManager.create(Index(BSONSerializationPack)(
            key = notExistingIndex.fieldNames.map(_ -> IndexType.Ascending).toSeq,
            name = Some(notExistingIndex.name),
            unique = notExistingIndex.unique,
            background = false,
            dropDups = false,
            sparse = false,
            version = Some(1),
            partialFilter = None,
            options = BSONDocument.empty
          ))
        }
      }
    }

    val collectionsCheck = List(
      CollectionConfig(RawJobSourceCollectionName, Set.empty),
      CollectionConfig(ScrapedJobCollectionName, Set.empty),
      CollectionConfig(JobParsingErrorCollectionName, Set.empty)
    )

    val checkF = for {
      db <- database
      currentCollections <- db.collectionNames.map(_.toSet)
      checkResult <- {
        Future.traverse(collectionsCheck) { collectionToEnsure =>
          logger.info(s"Checking collection ${collectionToEnsure.name}...")
          checkCollection(currentCollections, collectionToEnsure)
        }
      }
    } yield checkResult

    Try {
      Await.result(checkF, 3.minutes)
    } match {
      case Success(_) =>
        logger.info(s"Database connection established. All collections checked: [${collectionsCheck.map(_.name).mkString(", ")}]")
      case Failure(e) =>
        logger.error(s"Error when checking collections: ${collectionsCheck.map(_.name).mkString(", ")}", e)
        throw e
    }
  }

}

private case class IndexConfig(name: String, fieldNames: Set[String], unique: Boolean)

private case class CollectionConfig(name: String, indices: Set[IndexConfig])
