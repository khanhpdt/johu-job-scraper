package vn.johu.scraping

import scala.io.Source

import vn.johu.scraping.jsoup.{HtmlDoc, JSoup}

class JSoupMock(htmls: Seq[HtmlMock]) extends JSoup {
  override def get(url: String): HtmlDoc = {
    htmls.find(_.url == url) match {
      case Some(html) =>
        HtmlDoc.fromHtml(Source.fromResource(html.htmlFilePath).mkString)
      case None =>
        throw new IllegalArgumentException(s"Please provide html mock for url: $url")
    }
  }
}

object JSoupMock {
  def apply(htmls: Seq[HtmlMock]): JSoupMock = new JSoupMock(htmls)
}

case class HtmlMock(url: String, htmlFilePath: String)
