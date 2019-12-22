package vn.johu.scraping.models

import scala.util.Try

import reactivemongo.api.bson._

import vn.johu.scraping.models.RawJobSourceType.RawJobSourceType

object RawJobSourceType extends Enumeration {
  type RawJobSourceType = Value
  val Html, Json = Value
}

case class RawJobSource(id: Option[BSONObjectID], content: String, sourceType: RawJobSourceType)

object RawJobSource {

  implicit object RawJobSourceReader extends BSONDocumentReader[RawJobSource] {
    override def readDocument(doc: BSONDocument): Try[RawJobSource] = {
      Try {
        RawJobSource(
          id = doc.getAsOpt[BSONObjectID]("_id"),
          content = doc.getAsOpt[String]("content").get,
          sourceType = RawJobSourceType.withName(doc.getAsOpt[String]("sourceType").get)
        )
      }
    }
  }

  implicit object RawJobSourceWriter extends BSONDocumentWriter[RawJobSource] {
    override def writeTry(t: RawJobSource): Try[BSONDocument] = {
      Try {
        BSONDocument(
          "_id" -> t.id,
          "content" -> t.content,
          "sourceType" -> t.sourceType.toString
        )
      }
    }
  }

}
