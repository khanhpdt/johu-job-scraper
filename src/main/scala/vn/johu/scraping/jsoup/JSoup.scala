package vn.johu.scraping.jsoup

import scala.concurrent.{ExecutionContext, Future}

import org.jsoup.Jsoup

trait JSoup {

  def get(url: String): HtmlDoc

  def getAsync(url: String)(implicit ec: ExecutionContext): Future[HtmlDoc] = Future(get(url))
}

object JSoup extends JSoup {
  override def get(url: String): HtmlDoc = {
    HtmlDoc(Jsoup.connect(url).get())
  }
}