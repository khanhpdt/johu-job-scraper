package vn.johu.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, RootJsonFormat}

import vn.johu.scraping.models.RawJobSourceName
import vn.johu.scraping.scrapers.ScraperManager

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val rawJobSourceNameFormat: EnumJsonFormat[RawJobSourceName.type] =
    new EnumJsonFormat(RawJobSourceName)
  implicit val fixedScrapedJobsFormat: RootJsonFormat[ScraperManager.ParseLocalJobSources] =
    jsonFormat3(ScraperManager.ParseLocalJobSources)
  implicit val runScrapersFormat: RootJsonFormat[ScraperManager.RunScrapers] =
    jsonFormat1(ScraperManager.RunScrapers)

}

class EnumJsonFormat[T <: scala.Enumeration](enumObject: T) extends RootJsonFormat[T#Value] {
  override def write(obj: T#Value): JsValue = JsString(obj.toString)

  override def read(json: JsValue): T#Value = json match {
    case JsString(s) => enumObject.withName(s)
    case s => throw DeserializationException(s"Expected a string in this json value. Got ${s.toString}.")
  }
}