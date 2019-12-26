package vn.johu.http

import akka.actor.typed.ActorRef
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

import vn.johu.scraping.ScraperManager

class Routes(scraperManager: ActorRef[ScraperManager.Command]) {

  lazy val routes: Route = pathPrefix("operations") {
    concat {
      path("fixScrapedJobs") {
        post {
          complete(HttpEntity("Fixed OK"))
        }
      }
    }
  }

}
