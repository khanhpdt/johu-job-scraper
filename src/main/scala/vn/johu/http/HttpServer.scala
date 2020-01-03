package vn.johu.http

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer

import vn.johu.scraping.scrapers.JobScraperManager
import vn.johu.utils.Logging

object HttpServer extends Logging {

  private var serverBinding: ServerBinding = _

  def init(actorSystem: ActorSystem[Nothing], scraperManager: ActorRef[JobScraperManager.Command]): Unit = {
    logger.info("Starting HttpServer...")

    implicit val as: ActorSystem[Nothing] = actorSystem
    implicit val untypedSystem: akka.actor.ActorSystem = actorSystem.toUntyped
    implicit val am: ActorMaterializer = ActorMaterializer()(untypedSystem)

    val routes = new Routes(scraperManager)

    val port = 9090
    serverBinding = Await.result(Http().bindAndHandle(routes.routes, "0.0.0.0", port), 3.minutes)

    logger.info(s"HttpServer started and is listening on port $port")
  }

  def close(): Unit = {
    logger.info("Closing HttpServer. TBD.")
    serverBinding.terminate(3.minutes)
  }

}
