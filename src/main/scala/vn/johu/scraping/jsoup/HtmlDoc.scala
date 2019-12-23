package vn.johu.scraping.jsoup

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

}
