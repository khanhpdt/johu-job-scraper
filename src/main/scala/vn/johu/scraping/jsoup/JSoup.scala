package vn.johu.scraping.jsoup

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import org.jsoup.Jsoup

trait JSoup {

  def get(url: String): HtmlDoc

  def getAsync(url: String)(implicit ec: ExecutionContext): Future[HtmlDoc] = Future(get(url))

  def tryGetAsync(url: String)(implicit ec: ExecutionContext): Future[Try[HtmlDoc]] = Future(Try(get(url)))
}

object JSoup extends JSoup {
  override def get(url: String): HtmlDoc = {
    HtmlDoc(Jsoup.connect(url).get())
  }
}

object JSoupHelper {

  def getHtml(jSoup: JSoup, url: String)(implicit ec: ExecutionContext): Future[String] = {
    jSoup.getAsync(url).map(_.doc.outerHtml())
  }

}