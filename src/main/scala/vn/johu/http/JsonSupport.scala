package vn.johu.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, RootJsonFormat}

import vn.johu.scraping.ScraperManager
import vn.johu.scraping.models.RawJobSourceName

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val rawJobSourceNameFormat: EnumJsonFormat[RawJobSourceName.type] =
    new EnumJsonFormat(RawJobSourceName)
  implicit val fixedScrapedJobsFormat: RootJsonFormat[ScraperManager.ParseLocalJobSources] =
    jsonFormat3(ScraperManager.ParseLocalJobSources)

}

class EnumJsonFormat[T <: scala.Enumeration](enumObject: T) extends RootJsonFormat[T#Value] {
  override def write(obj: T#Value): JsValue = JsString(obj.toString)

  override def read(json: JsValue): T#Value = json match {
    case JsString(s) => enumObject.withName(s)
    case s => throw DeserializationException(s"Expected a string in this json value. Got ${s.toString}.")
  }
}