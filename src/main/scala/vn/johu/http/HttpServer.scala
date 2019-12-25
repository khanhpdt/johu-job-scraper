package vn.johu.http

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer

import vn.johu.utils.Logging

object HttpServer extends Logging {

  def init(actorSystem: ActorSystem[Nothing]): Unit = {
    logger.info("Starting HttpServer...")

    implicit val as: ActorSystem[Nothing] = actorSystem
    implicit val untypedSystem: akka.actor.ActorSystem = actorSystem.toUntyped
    implicit val am: ActorMaterializer = ActorMaterializer()(untypedSystem)

    val routes = new Routes()
    Http().bindAndHandle(routes.routes, "localhost", 9090)
  }

  def close(): Unit = {
    logger.info("Closing HttpServer. TBD.")
  }

}
