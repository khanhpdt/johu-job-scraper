package vn.johu.scraping.jsoup

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

class HtmlDoc(val doc: Document) {

  def select(cssQuery: String): List[HtmlElem] = {
    doc.select(cssQuery).iterator().asScala.toList.map(e => HtmlElem(e))
  }

  def selectFirst(cssQuery: String): Option[HtmlElem] = {
    Option(doc.selectFirst(cssQuery)).map(e => HtmlElem(e))
  }

}

object HtmlDoc {

  def apply(d: Document) = new HtmlDoc(d)

  def fromHtml(html: String): HtmlDoc = {
    HtmlDoc(Jsoup.parse(html))
  }

  def fromUrl(url: String): HtmlDoc = {
    HtmlDoc(Jsoup.connect(url).get)
  }

  def fromUrlAsync(url: String)(implicit ec: ExecutionContext): Future[HtmlDoc] = Future {
    HtmlDoc(Jsoup.connect(url).get)
  }

}
