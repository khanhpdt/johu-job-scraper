package vn.johu.http

import scala.concurrent.duration._

import akka.actor.Scheduler
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout

import vn.johu.scraping.ScraperManager

class Routes(scraperManager: ActorRef[ScraperManager.Command])(implicit system: ActorSystem[_]) extends JsonSupport {

  // asking someone requires a timeout and a scheduler, if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.seconds

  // implicit scheduler only needed in 2.5
  // in 2.6 having an implicit typed ActorSystem in scope is enough
  implicit val scheduler: Scheduler = system.scheduler

  lazy val routes: Route = pathPrefix("operations") {
    concat {
      path("fixScrapedJobs") {
        post {
          entity(as[ScraperManager.FixScrapedJobs]) { msg =>
            complete {
              scraperManager ! msg
              HttpResponse()
            }
          }
        }
      }
    }
  }

}
