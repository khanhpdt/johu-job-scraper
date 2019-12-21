package vn.johu.scraping.itviec

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import reactivemongo.api.bson.BSONObjectID

import vn.johu.scraping.Scraper.{Command, ParseDoc, StartScraping}
import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem}
import vn.johu.scraping.models.ScrapedJob
import vn.johu.scraping.{Scraper, ScrapingCoordinator}
import vn.johu.utils.Logging

class ItViecScraper(context: ActorContext[Scraper.Command])
  extends AbstractBehavior[Scraper.Command]
    with Logging {

  implicit val ec: ExecutionContextExecutor = context.system.executionContext

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case StartScraping(replyTo) =>
        scrape(replyTo)
        this
      case ParseDoc(doc, replyTo) =>
        parseDoc(doc, replyTo)
        this
    }
  }

  private def scrape(replyTo: ActorRef[ScrapingCoordinator.JobsScraped]): Unit = {
    val url = "https://itviec.com/it-jobs?page=1"

    logger.info(s"Start scraping at url: $url")

    val scrapeResultF =
      for {
        htmlDoc <- HtmlDoc.fromUrlAsync(url)
      } yield {
        parseDoc(htmlDoc, replyTo)
      }

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from url: $url", ex)
      case Success(_) =>
        logger.info(s"Successfully scrape from url: $url")
    }
  }

  private def parseDoc(doc: HtmlDoc, replyTo: ActorRef[ScrapingCoordinator.JobsScraped]): Unit = {
    val parseResult = for {rawHtmlId <- saveRawHtml(doc)} yield {
      val jobElements = doc.select("#search-results #jobs div.job")
      val jobs = jobElements.map(parseJobElement(_, rawHtmlId))

      prepareNextScrape(jobs, replyTo)

      replyTo ! ScrapingCoordinator.JobsScraped(jobs)
    }

    parseResult.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when parsing", ex)
      case Success(_) =>
        logger.info(s"Parse doc successfully.")
    }
  }

  private def saveRawHtml(htmlDoc: HtmlDoc): Future[BSONObjectID] = {
    Future.successful(BSONObjectID.generate())
  }

  private def parseJobElement(jobElem: HtmlElem, rawHtmlId: BSONObjectID): ScrapedJob = {

  }

  private def prepareNextScrape(scrapedJobs: List[ScrapedJob], replyTo: ActorRef[ScrapingCoordinator.JobsScraped]): Unit = {

  }

}

object ItViecScraper {
  def apply(): Behavior[Scraper.Command] = {
    Behaviors.setup[Scraper.Command](ctx => new ItViecScraper(ctx))
  }
}
