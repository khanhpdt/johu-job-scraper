package vn.johu.persistence

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.api.{AsyncDriver, DefaultDB, MongoConnection}

import vn.johu.scraping.models.{RawJobSource, ScrapedJob}
import vn.johu.utils.{Configs, Logging, TryHelper}

object MongoDb extends TryHelper with Logging {

  private val ScrapedJobCollectionName = "scraped_job"
  private val ScrapedJobDetailsCollectionName = "scraped_job_details"
  private val RawJobSourceCollectionName = "raw_job_source"
  private val JobParsingErrorCollectionName = "job_parsing_error"
  private val JobScrapingHistoryCollectionName = "job_scraping_history"

  private var dbName: String = _
  private var mongoConnection: MongoConnection = _

  def init(config: Config)(implicit ec: ExecutionContext): Unit = {
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

  def close()(implicit ec: ExecutionContext): Unit = {
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
  def database(implicit ec: ExecutionContext): Future[DefaultDB] = mongoConnection.database(dbName)

  def scrapedJobColl(implicit ec: ExecutionContext): Future[BSONCollection] = collection(ScrapedJobCollectionName)

  def scrapedJobDetailsColl(implicit ec: ExecutionContext): Future[BSONCollection] = collection(ScrapedJobDetailsCollectionName)

  def rawJobSourceColl(implicit ec: ExecutionContext): Future[BSONCollection] = collection(RawJobSourceCollectionName)

  def jobParsingErrorColl(implicit ec: ExecutionContext): Future[BSONCollection] = collection(JobParsingErrorCollectionName)

  def jobScrapingHistoryColl(implicit ec: ExecutionContext): Future[BSONCollection] = collection(JobScrapingHistoryCollectionName)

  def allCollections(implicit ec: ExecutionContext) =
    List(scrapedJobColl, rawJobSourceColl, jobParsingErrorColl, jobScrapingHistoryColl)

  private def collection(name: String)(implicit ec: ExecutionContext) = database.map(_.collection[BSONCollection](name))

  private def checkDb()(implicit ec: ExecutionContext): Unit = {
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
      CollectionConfig(RawJobSourceCollectionName,
        Set(
          IndexConfig(
            name = "source_scraping_ts",
            fieldNames = Set(RawJobSource.Fields.sourceName, RawJobSource.Fields.scrapingTs),
            unique = false
          )
        )
      ),
      CollectionConfig(
        ScrapedJobCollectionName,
        Set(
          IndexConfig(
            name = "job_url",
            fieldNames = Set(ScrapedJob.Fields.url),
            unique = false
          ),
          IndexConfig(
            name = "job_key_per_source",
            fieldNames = Set(ScrapedJob.Fields.rawJobSourceName, ScrapedJob.Fields.url),
            unique = true
          )
        )
      ),
      CollectionConfig(ScrapedJobDetailsCollectionName, Set.empty),
      CollectionConfig(JobParsingErrorCollectionName, Set.empty),
      CollectionConfig(JobScrapingHistoryCollectionName, Set.empty),
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
