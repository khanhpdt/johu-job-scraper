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
  locations: Set[String],
  rawJobSourceName: RawJobSourceName,
  rawJobSourceId: BSONObjectID
)

object ScrapedJob {

  implicit object ScrapedJobReader extends BSONDocumentReader[ScrapedJob] {
    override def readDocument(doc: BSONDocument): Try[ScrapedJob] = {
      Try {
        ScrapedJob(
          id = doc.getAsOpt[BSONObjectID](Fields.id),
          url = doc.getAsOpt[String](Fields.url).get,
          title = doc.getAsOpt[String](Fields.title).get,
          tags = doc.getAsOpt[Array[String]](Fields.tags).get.toSet,
          postingDate = doc.getAsOpt[BSONDateTime](Fields.postingDate).get,
          company = doc.getAsOpt[String](Fields.company).get,
          locations = doc.getAsOpt[Array[String]](Fields.locations).get.toSet,
          rawJobSourceName = RawJobSourceName.withName(doc.getAsOpt[String](Fields.rawJobSourceName).get),
          rawJobSourceId = doc.getAsOpt[BSONObjectID](Fields.rawJobSourceId).get
        )
      }
    }
  }

  implicit object ScrapedJobWriter extends BSONDocumentWriter[ScrapedJob] {
    override def writeTry(job: ScrapedJob): Try[BSONDocument] = {
      Try {
        BSONDocument(
          Fields.id -> job.id,
          Fields.url -> job.url,
          Fields.title -> job.title,
          Fields.tags -> job.tags,
          Fields.postingDate -> job.postingDate,
          Fields.company -> job.company,
          Fields.locations -> job.locations,
          Fields.rawJobSourceName -> job.rawJobSourceName.toString,
          Fields.rawJobSourceId -> job.rawJobSourceId
        )
      }
    }
  }

  implicit val encoder: Encoder[ScrapedJob] = (job: ScrapedJob) => {
    Json.obj(
      Fields.id -> Json.fromString(job.id.get.stringify),
      Fields.url -> Json.fromString(job.url),
      Fields.title -> Json.fromString(job.title),
      Fields.tags -> Json.fromValues(job.tags.map(Json.fromString)),
      Fields.postingDate -> Json.fromLong(job.postingDate.toLong.get),
      Fields.company -> Json.fromString(job.company),
      Fields.locations -> Json.fromValues(job.locations.map(Json.fromString))
    )
  }

  object Fields {
    val id = "_id"
    val url = "url"
    val title = "title"
    val tags = "tags"
    val postingDate = "postingDate"
    val company = "company"
    val locations = "locations"
    val rawJobSourceName = "rawJobSourceName"
    val rawJobSourceId = "rawJobSourceId"
  }

}