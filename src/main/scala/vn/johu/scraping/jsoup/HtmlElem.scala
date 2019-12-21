package vn.johu.scraping.jsoup

import scala.jdk.CollectionConverters._

import org.jsoup.nodes.Element

class HtmlElem(val elem: Element) {

  def selectFirst(cssQuery: String): Option[HtmlElem] = Option(elem.selectFirst(cssQuery)).map(HtmlElem(_))

  def nextSibling: Option[HtmlElem] = Option(elem.nextElementSibling).map(e => HtmlElem(e))

  def attr(attrKey: String): Option[String] = Option(elem.attr(attrKey))

  def select(cssQuery: String): List[HtmlElem] = {
    elem.select(cssQuery).iterator().asScala.toList.map(e => HtmlElem(e))
  }
}

object HtmlElem {

  def apply(e: Element) = new HtmlElem(e)

}
