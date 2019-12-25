package vn.johu.http

import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

class Routes {

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
