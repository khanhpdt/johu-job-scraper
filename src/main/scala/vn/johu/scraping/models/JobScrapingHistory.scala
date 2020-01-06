package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONObjectID}

import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName

case class JobScrapingHistory(
  id: BSONObjectID,
  rawJobSourceName: RawJobSourceName,
  startTime: BSONDateTime,
  rawJobSourceId: Option[BSONObjectID] = None,
  newJobIds: List[BSONObjectID] = Nil,
  existingJobIds: List[BSONObjectID] = Nil,
  parsingErrorIds: List[BSONObjectID] = Nil,
  scrapingError: Option[String] = None,
  endTime: Option[BSONDateTime] = None
)

object JobScrapingHistory {

  implicit object JobScrapingHistoryReader extends BSONDocumentReader[JobScrapingHistory] {
    override def readDocument(doc: BSONDocument): Try[JobScrapingHistory] = {
      Try {
        JobScrapingHistory(
          id = doc.getAsOpt[BSONObjectID](Fields.id).get,
          rawJobSourceName = RawJobSourceName.withName(doc.getAsOpt[String](Fields.rawJobSourceName).get),
          startTime = doc.getAsOpt[BSONDateTime](Fields.startTime).get,
          rawJobSourceId = doc.getAsOpt[BSONObjectID](Fields.rawJobSourceId),
          newJobIds = doc.getAsOpt[Array[BSONObjectID]](Fields.newJobIds).get.toList,
          existingJobIds = doc.getAsOpt[Array[BSONObjectID]](Fields.existingJobIds).get.toList,
          parsingErrorIds = doc.getAsOpt[Array[BSONObjectID]](Fields.parsingErrorIds).get.toList,
          scrapingError = doc.getAsOpt[String](Fields.scrapingError),
          endTime = doc.getAsOpt[BSONDateTime](Fields.endTime)
        )
      }
    }
  }

  implicit object JobScrapingHistoryWriter extends BSONDocumentWriter[JobScrapingHistory] {
    override def writeTry(t: JobScrapingHistory): Try[BSONDocument] = {
      Try {
        BSONDocument(
          Fields.id -> t.id,
          Fields.startTime -> t.startTime,
          Fields.rawJobSourceName -> t.rawJobSourceName.toString,
          Fields.rawJobSourceId -> t.rawJobSourceId,
          Fields.newJobIds -> t.newJobIds,
          Fields.existingJobIds -> t.existingJobIds,
          Fields.parsingErrorIds -> t.parsingErrorIds,
          Fields.scrapingError -> t.scrapingError,
          Fields.endTime -> t.endTime
        )
      }
    }
  }

  object Fields {
    val id = "_id"
    val rawJobSourceName = "rawJobSourceName"
    val startTime = "startTime"
    val rawJobSourceId = "rawJobSourceId"
    val newJobIds = "newJobIds"
    val existingJobIds = "existingJobIds"
    val parsingErrorIds = "parsingErrorIds"
    val scrapingError = "scrapingError"
    val endTime = "endTime"
  }

}
