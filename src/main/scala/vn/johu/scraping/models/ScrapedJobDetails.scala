package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONObjectID}

case class ScrapedJobDetails(
  id: BSONObjectID,
  scrapedJobId: BSONObjectID,
  html: Option[String],
  scrapingError: Option[String],
  createdTs: BSONDateTime
)

object ScrapedJobDetails {

  implicit object ScrapedJobDetailsReader extends BSONDocumentReader[ScrapedJobDetails] {
    override def readDocument(doc: BSONDocument): Try[ScrapedJobDetails] = {
      Try {
        ScrapedJobDetails(
          id = doc.getAsOpt[BSONObjectID]("_id").get,
          scrapedJobId = doc.getAsOpt[BSONObjectID]("scrapedJobId").get,
          html = doc.getAsOpt[String]("html"),
          scrapingError = doc.getAsOpt[String]("scrapingError"),
          createdTs = doc.getAsOpt[BSONDateTime]("createdTs").get
        )
      }
    }
  }

  implicit object ScrapedJobDetailsWriter extends BSONDocumentWriter[ScrapedJobDetails] {
    override def writeTry(t: ScrapedJobDetails): Try[BSONDocument] = {
      Try {
        BSONDocument(
          "_id" -> t.id,
          "scrapedJobId" -> t.scrapedJobId,
          "html" -> t.html,
          "scrapingError" -> t.scrapingError,
          "createdTs" -> t.createdTs,
        )
      }
    }
  }

}