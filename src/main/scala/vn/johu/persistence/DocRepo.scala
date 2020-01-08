package vn.johu.persistence

import scala.concurrent.{ExecutionContext, Future}

import reactivemongo.api.Cursor
import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONObjectID, ElementProducer, document}
import reactivemongo.api.commands.UpdateWriteResult

import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName
import vn.johu.scraping.models.{JobParsingError, JobScrapingHistory, RawJobSource, ScrapedJob}
import vn.johu.utils.DateUtils

object DocRepo {

  def insertJobs(jobs: List[ScrapedJob])(implicit ec: ExecutionContext): Future[List[ScrapedJob]] = {
    if (jobs.isEmpty) {
      Future.successful(Nil)
    } else {
      MongoDb.scrapedJobColl.flatMap { coll =>
        val jobsToInsert = jobs.map { j =>
          val now = BSONDateTime(DateUtils.nowMillis())
          setTechnicalFields(j, createdTs = Some(now), modifiedTs = Some(now))
        }
        coll.insert(ordered = false).many(jobsToInsert).map(_ => jobsToInsert)
      }
    }
  }

  private def setTechnicalFields(
    job: ScrapedJob,
    createdTs: Option[BSONDateTime] = None,
    modifiedTs: Option[BSONDateTime] = None
  ) = {
    var result = job

    createdTs.foreach { ts =>
      result = result.copy(createdTs = Some(ts))
    }

    modifiedTs.foreach { ts =>
      result = result.copy(modifiedTs = Some(ts))
    }

    result
  }

  def updateJobs(jobs: List[ScrapedJob])(implicit ec: ExecutionContext): Future[Unit] = {
    if (jobs.isEmpty) {
      Future.successful(())
    } else {
      import ScrapedJob.Fields
      val updateResultF = MongoDb.scrapedJobColl.flatMap { coll =>
        val updateBuilder = coll.update(ordered = false)
        val updates = Future.sequence(jobs.map { job =>
          updateBuilder.element(
            q = document(Fields.id -> job.id.get),
            u = document(
              "$set" -> document(
                Fields.url -> job.url,
                Fields.title -> job.title,
                Fields.tags -> job.tags,
                Fields.postingDate -> job.postingDate,
                Fields.otherPostingDates -> job.otherPostingDates,
                Fields.company -> job.company,
                Fields.locations -> job.locations,
                Fields.rawJobSourceName -> job.rawJobSourceName.toString,
                Fields.rawJobSourceId -> job.rawJobSourceId,
                Fields.modifiedTs -> BSONDateTime(DateUtils.nowMillis())
              )
            ),
            upsert = false,
            multi = false
          )
        })
        updates.flatMap(updateBuilder.many)
      }
      updateResultF.map(_ => ())
    }
  }

  def findJobsWithFields(
    jobUrls: Set[String],
    rawJobSourceName: RawJobSourceName,
    fields: Set[String]
  )(implicit ec: ExecutionContext): Future[List[BSONDocument]] = {
    MongoDb.scrapedJobColl.flatMap { coll =>
      coll.find(
        selector = document(
          ScrapedJob.Fields.url -> document("$in" -> jobUrls),
          ScrapedJob.Fields.rawJobSourceName -> rawJobSourceName.toString
        ),
        projection = Some(document(
          fields.toSeq.map { f =>
            val result: ElementProducer = (f, 1)
            result
          }: _*)
        )
      ).cursor[BSONDocument]().collect[List](jobUrls.size, Cursor.FailOnError[List[BSONDocument]]())
    }
  }

  def findJobsByUrls(
    jobUrls: Set[String],
    rawJobSourceName: RawJobSourceName
  )(implicit ec: ExecutionContext): Future[List[ScrapedJob]] = {
    MongoDb.scrapedJobColl.flatMap { coll =>
      coll.find(
        selector = document(
          ScrapedJob.Fields.url -> document("$in" -> jobUrls),
          ScrapedJob.Fields.rawJobSourceName -> rawJobSourceName.toString
        ),
        projection = Option.empty[BSONDocument]
      ).cursor[ScrapedJob]().collect[List](jobUrls.size, Cursor.FailOnError[List[ScrapedJob]]())
    }
  }

  def findAllJobs()(implicit ec: ExecutionContext): Future[List[ScrapedJob]] = {
    MongoDb.scrapedJobColl.flatMap { coll =>
      coll.find(
        selector = document(),
        projection = Option.empty[BSONDocument]
      ).cursor[ScrapedJob]().collect[List](-1, Cursor.FailOnError[List[ScrapedJob]]())
    }
  }

  def insertErrors(errors: List[JobParsingError])(implicit ec: ExecutionContext): Future[List[JobParsingError]] = {
    if (errors.isEmpty) {
      Future.successful(Nil)
    } else {
      MongoDb.jobParsingErrorColl.flatMap { coll =>
        coll.insert(ordered = false).many(errors).map(_ => errors)
      }
    }
  }

  def findRawJobSources(
    rawJobSourceName: RawJobSourceName,
    startTs: Option[String],
    endTs: Option[String]
  )(implicit ec: ExecutionContext): Future[List[RawJobSource]] = {
    val startTimeOpt = startTs.map(DateUtils.parseDateTime)
    val endTimeOpt = endTs.map(DateUtils.parseDateTime)

    var query = document(RawJobSource.Fields.sourceName -> rawJobSourceName.toString)
    startTimeOpt.foreach(t => query ++= RawJobSource.Fields.scrapingTs -> document("$gte" -> t))
    endTimeOpt.foreach(t => query ++= RawJobSource.Fields.scrapingTs -> document("$lte" -> t))

    MongoDb.rawJobSourceColl.flatMap { coll =>
      coll.find(selector = query, projection = Option.empty[BSONDocument])
        .cursor[RawJobSource]()
        .collect[List](-1, Cursor.FailOnError[List[RawJobSource]]())
    }
  }

  def insertRawJobSource(
    rawJobSourceName: RawJobSourceName,
    page: Int,
    content: String
  )(implicit ec: ExecutionContext): Future[RawJobSource] = {
    MongoDb.rawJobSourceColl.flatMap { coll =>
      val source = RawJobSource(
        id = Some(BSONObjectID.generate),
        page = page,
        sourceName = rawJobSourceName,
        content = content,
        scrapingTs = BSONDateTime(DateUtils.nowMillis())
      )
      coll.insert(ordered = false).one[RawJobSource](source).map(_ => source)
    }
  }

  def insertScrapingHistory(hist: JobScrapingHistory)(implicit ec: ExecutionContext): Future[JobScrapingHistory] = {
    MongoDb.jobScrapingHistoryColl.flatMap { coll =>
      coll.insert(ordered = false).one(hist).map(_ => hist)
    }
  }

  def saveScrapingHistory(hist: JobScrapingHistory)(implicit ec: ExecutionContext): Future[UpdateWriteResult] = {
    MongoDb.jobScrapingHistoryColl.flatMap { coll =>
      coll.update(ordered = false).one(
        q = document(JobScrapingHistory.Fields.id -> hist.id),
        u = hist
      )
    }
  }

}
