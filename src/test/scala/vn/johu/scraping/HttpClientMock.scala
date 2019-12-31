package vn.johu.scraping

import scala.concurrent.Future
import scala.io.Source

import spray.json._

import vn.johu.http.HttpClient

class HttpClientMock(responseMocks: Seq[HttpResponseMock], mockSequentially: Boolean = false) extends HttpClient {

  private var requestNumber = 0

  override def post(url: String, body: Array[Byte]): Future[JsObject] = {
    val mock =
      if (mockSequentially) {
        val res = responseMocks(requestNumber)
        requestNumber += 1
        res
      } else {
        getMock(url)
      }
    Future.successful {
      Source.fromResource(mock.jsonFilePath).mkString.parseJson.asJsObject
    }
  }

  private def getMock(url: String) = {
    responseMocks.find(_.url == url) match {
      case Some(found) =>
        found
      case None =>
        responseMocks.find(_.url.isEmpty)
          .getOrElse(throw new IllegalArgumentException(s"Please provide http response mock for url: $url"))
    }
  }

}

object HttpClientMock {
  def apply(mocks: Seq[HttpResponseMock], mockSequentially: Boolean = false) = new HttpClientMock(mocks, mockSequentially)
}

case class HttpResponseMock(url: String = "", jsonFilePath: String)