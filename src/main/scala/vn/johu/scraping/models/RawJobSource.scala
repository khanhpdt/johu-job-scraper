package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson._

import vn.johu.scraping.models.RawJobSourceName.RawJobSourceName

object RawJobSourceName extends Enumeration {
  type RawJobSourceName = Value
  val ItViec, VietnamWorks = Value
}

case class RawJobSource(
  id: Option[BSONObjectID],
  page: Int,
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
          page = doc.getAsOpt[Int](Fields.page).get,
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
          Fields.page -> t.page,
          Fields.sourceName -> t.sourceName.toString,
          Fields.content -> t.content,
          Fields.scrapingTs -> t.scrapingTs
        )
      }
    }
  }

  object Fields {
    val id = "_id"
    val page = "page"
    val sourceName = "sourceName"
    val content = "content"
    val scrapingTs = "scrapingTs"
  }

}
