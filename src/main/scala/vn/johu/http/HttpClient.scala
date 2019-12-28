package vn.johu.http

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.ByteString
import spray.json.{DefaultJsonProtocol, JsObject}

import vn.johu.utils.Logging

trait HttpClient {

  def post(url: String, body: Array[Byte]): Future[JsObject]

}

object HttpClient extends HttpClient with Logging with SprayJsonSupport with DefaultJsonProtocol {

  private implicit var untypedActorSystem: ActorSystem = _
  private implicit var am: ActorMaterializer = _
  private implicit var ec: ExecutionContext = _

  def init(actorSystem: akka.actor.typed.ActorSystem[_]): Unit = {
    logger.info("Initializing HttpClient...")

    untypedActorSystem = actorSystem.toUntyped
    am = ActorMaterializer()(untypedActorSystem)
    ec = actorSystem.executionContext
  }

  override def post(url: String, body: Array[Byte]): Future[JsObject] = {
    val responseF = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = Uri(url),
        headers = Seq(headers.Accept(MediaRanges.`application/*`)),
        entity = HttpEntity(ContentTypes.`application/json`, body)
      )
    ).map { response =>
      if (response.status.isFailure()) {
        response.discardEntityBytes()
        throw new IllegalStateException(s"Request failed. Got http status ${response.status}.")
      } else {
        response
      }
    }

    for {
      response <- responseF
      entity <- response.entity.toStrict(3.seconds)
      byteS <- consumeEntity(entity)
      entityBody <- Unmarshal(byteS).to[JsObject]
    } yield {
      entityBody
    }
  }

  private def consumeEntity(e: HttpEntity) = {
    e.dataBytes.runFold(ByteString.empty) { case (acc, b) => acc ++ b }
  }

}