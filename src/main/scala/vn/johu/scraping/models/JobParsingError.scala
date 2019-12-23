package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONObjectID}

case class JobParsingError(
  id: BSONObjectID,
  rawSourceContent: String,
  errors: List[String],
  rawJobSourceId: BSONObjectID,
  scrapingTs: BSONDateTime
)

object JobParsingError {

  implicit object JobParsingErrorReader extends BSONDocumentReader[JobParsingError] {
    override def readDocument(doc: BSONDocument): Try[JobParsingError] = {
      Try {
        JobParsingError(
          id = doc.getAsOpt[BSONObjectID]("_id").get,
          rawSourceContent = doc.getAsOpt[String]("rawSourceContent").get,
          errors = doc.getAsOpt[Array[String]]("errors").get.toList,
          rawJobSourceId = doc.getAsOpt[BSONObjectID]("rawJobSourceId").get,
          scrapingTs = doc.getAsOpt[BSONDateTime]("scrapingTs").get
        )
      }
    }
  }

  implicit object JobParsingErrorWriter extends BSONDocumentWriter[JobParsingError] {
    override def writeTry(t: JobParsingError): Try[BSONDocument] = {
      Try {
        BSONDocument(
          "_id" -> t.id,
          "rawSourceContent" -> t.rawSourceContent,
          "errors" -> t.errors,
          "rawJobSourceId" -> t.rawJobSourceId,
          "scrapingTs" -> t.scrapingTs
        )
      }
    }
  }

}