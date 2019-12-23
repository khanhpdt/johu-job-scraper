package vn.johu.scraping.models

import scala.util.Try

import io.circe.{Encoder, Json}
import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONObjectID}

case class ScrapedJob(
  id: Option[BSONObjectID],
  url: String,
  title: String,
  tags: Set[String],
  postingDate: BSONDateTime,
  company: String,
  location: String,
  rawJobSourceId: BSONObjectID
)

object ScrapedJobField extends Enumeration {
  type ScrapedJobField = Value
  val url, title, tags, postingDate, company, location = Value
}

object ScrapedJob {

  implicit object ScrapedJobReader extends BSONDocumentReader[ScrapedJob] {
    override def readDocument(doc: BSONDocument): Try[ScrapedJob] = {
      Try {
        ScrapedJob(
          id = doc.getAsOpt[BSONObjectID]("_id"),
          url = doc.getAsOpt[String]("url").get,
          title = doc.getAsOpt[String]("title").get,
          tags = doc.getAsOpt[Array[String]]("tags").get.toSet,
          postingDate = doc.getAsOpt[BSONDateTime]("postingDate").get,
          company = doc.getAsOpt[String]("company").get,
          location = doc.getAsOpt[String]("location").get,
          rawJobSourceId = doc.getAsOpt[BSONObjectID]("rawJobSourceId").get
        )
      }
    }
  }

  implicit object ScrapedJobWriter extends BSONDocumentWriter[ScrapedJob] {
    override def writeTry(job: ScrapedJob): Try[BSONDocument] = {
      Try {
        BSONDocument(
          "_id" -> job.id,
          "url" -> job.url,
          "title" -> job.title,
          "tags" -> job.tags,
          "postingDate" -> job.postingDate,
          "company" -> job.company,
          "location" -> job.location,
          "rawJobSourceId" -> job.rawJobSourceId
        )
      }
    }
  }

  implicit val encoder: Encoder[ScrapedJob] = (job: ScrapedJob) => {
    Json.obj(
      "_id" -> Json.fromString(job.id.get.stringify),
      "url" -> Json.fromString(job.url),
      "title" -> Json.fromString(job.title),
      "tags" -> Json.fromValues(job.tags.map(Json.fromString)),
      "postingDate" -> Json.fromLong(job.postingDate.toLong.get),
      "company" -> Json.fromString(job.company),
      "location" -> Json.fromString(job.location),
      "rawJobSourceId" -> Json.fromString(job.rawJobSourceId.stringify)
    )
  }

}