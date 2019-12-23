package vn.johu.scraping.models

import scala.util.Try

import io.circe.{Encoder, Json}
import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONDocumentReader, BSONDocumentWriter, BSONObjectID}

import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName

case class ScrapedJob(
  id: Option[BSONObjectID],
  url: String,
  title: String,
  tags: Set[String],
  postingDate: BSONDateTime,
  company: String,
  location: String,
  rawJobSourceName: RawJobSourceName,
  rawJobSourceId: BSONObjectID
)

object ScrapedJobParsingField extends Enumeration {
  type ScrapedJobField = Value
  val url, title, tags, postingDate, company, location = Value
}

object ScrapedJobField {
  val id = "_id"
  val url = "url"
  val title = "title"
  val tags = "tags"
  val postingDate = "postingDate"
  val company = "company"
  val location = "location"
  val rawJobSourceName = "rawJobSourceName"
  val rawJobSourceId = "rawJobSourceId"
}

object ScrapedJob {

  implicit object ScrapedJobReader extends BSONDocumentReader[ScrapedJob] {
    override def readDocument(doc: BSONDocument): Try[ScrapedJob] = {
      Try {
        ScrapedJob(
          id = doc.getAsOpt[BSONObjectID](ScrapedJobField.id),
          url = doc.getAsOpt[String](ScrapedJobField.url).get,
          title = doc.getAsOpt[String](ScrapedJobField.title).get,
          tags = doc.getAsOpt[Array[String]](ScrapedJobField.tags).get.toSet,
          postingDate = doc.getAsOpt[BSONDateTime](ScrapedJobField.postingDate).get,
          company = doc.getAsOpt[String](ScrapedJobField.company).get,
          location = doc.getAsOpt[String](ScrapedJobField.location).get,
          rawJobSourceName = RawJobSourceName.withName(doc.getAsOpt[String](ScrapedJobField.rawJobSourceName).get),
          rawJobSourceId = doc.getAsOpt[BSONObjectID](ScrapedJobField.rawJobSourceId).get
        )
      }
    }
  }

  implicit object ScrapedJobWriter extends BSONDocumentWriter[ScrapedJob] {
    override def writeTry(job: ScrapedJob): Try[BSONDocument] = {
      Try {
        BSONDocument(
          ScrapedJobField.id -> job.id,
          ScrapedJobField.url -> job.url,
          ScrapedJobField.title -> job.title,
          ScrapedJobField.tags -> job.tags,
          ScrapedJobField.postingDate -> job.postingDate,
          ScrapedJobField.company -> job.company,
          ScrapedJobField.location -> job.location,
          ScrapedJobField.rawJobSourceName -> job.rawJobSourceName.toString,
          ScrapedJobField.rawJobSourceId -> job.rawJobSourceId
        )
      }
    }
  }

  implicit val encoder: Encoder[ScrapedJob] = (job: ScrapedJob) => {
    Json.obj(
      ScrapedJobField.id -> Json.fromString(job.id.get.stringify),
      ScrapedJobField.url -> Json.fromString(job.url),
      ScrapedJobField.title -> Json.fromString(job.title),
      ScrapedJobField.tags -> Json.fromValues(job.tags.map(Json.fromString)),
      ScrapedJobField.postingDate -> Json.fromLong(job.postingDate.toLong.get),
      ScrapedJobField.company -> Json.fromString(job.company),
      ScrapedJobField.location -> Json.fromString(job.location)
    )
  }

}