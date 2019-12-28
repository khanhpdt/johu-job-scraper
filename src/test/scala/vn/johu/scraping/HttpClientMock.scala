package vn.johu.scraping

import scala.concurrent.Future
import scala.io.Source

import spray.json._

import vn.johu.http.HttpClient

class HttpClientMock(responseMocks: Seq[HttpResponseMock]) extends HttpClient {

  override def post(url: String, body: Array[Byte]): Future[JsObject] = {
    responseMocks.find(_.url == url) match {
      case Some(mock) =>
        Future.successful {
          Source.fromResource(mock.jsonFilePath).mkString.parseJson.asJsObject
        }
      case None =>
        throw new IllegalArgumentException(s"Please provide http response mock for url: $url")
    }
  }

}

case class HttpResponseMock(url: String, jsonFilePath: String)