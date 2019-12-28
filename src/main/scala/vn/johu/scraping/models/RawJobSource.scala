package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson._

import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName

object RawJobSourceType extends Enumeration {
  type RawJobSourceType = Value
  val Html, Json = Value
}

object RawJobSourceName extends Enumeration {
  type RawJobSourceName = Value
  val ItViec, VietnamWorks = Value
}

case class RawJobSource(
  id: Option[BSONObjectID],
  url: String,
  sourceName: RawJobSourceName,
  content: String,
  scrapingTs: BSONDateTime
)

object RawJobSource {

  implicit object RawJobSourceReader extends BSONDocumentReader[RawJobSource] {
    override def readDocument(doc: BSONDocument): Try[RawJobSource] = {
      Try {
        RawJobSource(
          id = doc.getAsOpt[BSONObjectID](Fields.id),
          url = doc.getAsOpt[String](Fields.url).get,
          sourceName = RawJobSourceName.withName(doc.getAsOpt[String](Fields.sourceName).get),
          content = doc.getAsOpt[String](Fields.content).get,
          scrapingTs = doc.getAsOpt[BSONDateTime](Fields.scrapingTs).get
        )
      }
    }
  }

  implicit object RawJobSourceWriter extends BSONDocumentWriter[RawJobSource] {
    override def writeTry(t: RawJobSource): Try[BSONDocument] = {
      Try {
        BSONDocument(
          Fields.id -> t.id,
          Fields.url -> t.url,
          Fields.sourceName -> t.sourceName.toString,
          Fields.content -> t.content,
          Fields.scrapingTs -> t.scrapingTs
        )
      }
    }
  }

  object Fields {
    val id = "_id"
    val url = "url"
    val sourceName = "sourceName"
    val content = "content"
    val scrapingTs = "scrapingTs"
  }

}
