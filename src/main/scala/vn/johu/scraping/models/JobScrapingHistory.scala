package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONObjectID}

import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName

case class JobScrapingHistory(
  id: BSONObjectID,
  rawJobSourceId: BSONObjectID,
  rawJobSourceName: RawJobSourceName,
  newJobIds: List[BSONObjectID],
  existingJobIds: List[BSONObjectID],
  errorIds: List[BSONObjectID],
  startTime: BSONDateTime,
  endTime: BSONDateTime
)

object JobScrapingHistory {

  implicit object JobScrapingHistoryReader extends BSONDocumentReader[JobScrapingHistory] {
    override def readDocument(doc: BSONDocument): Try[JobScrapingHistory] = {
      Try {
        JobScrapingHistory(
          id = doc.getAsOpt[BSONObjectID]("_id").get,
          rawJobSourceId = doc.getAsOpt[BSONObjectID]("rawJobSourceId").get,
          rawJobSourceName = RawJobSourceName.withName(doc.getAsOpt[String]("rawJobSourceName").get),
          newJobIds = doc.getAsOpt[Array[BSONObjectID]]("newJobIds").get.toList,
          existingJobIds = doc.getAsOpt[Array[BSONObjectID]]("existingJobIds").get.toList,
          errorIds = doc.getAsOpt[Array[BSONObjectID]]("errorIds").get.toList,
          startTime = doc.getAsOpt[BSONDateTime]("startTime").get,
          endTime = doc.getAsOpt[BSONDateTime]("endTime").get
        )
      }
    }
  }

  implicit object JobScrapingHistoryWriter extends BSONDocumentWriter[JobScrapingHistory] {
    override def writeTry(t: JobScrapingHistory): Try[BSONDocument] = {
      Try {
        BSONDocument(
          "_id" -> t.id,
          "rawJobSourceId" -> t.rawJobSourceId,
          "rawJobSourceName" -> t.rawJobSourceName.toString,
          "newJobIds" -> t.newJobIds,
          "existingJobIds" -> t.existingJobIds,
          "errorIds" -> t.errorIds,
          "startTime" -> t.startTime,
          "endTime" -> t.endTime
        )
      }
    }
  }

}
