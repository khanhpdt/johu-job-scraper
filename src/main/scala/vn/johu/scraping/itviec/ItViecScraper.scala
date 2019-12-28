package vn.johu.scraping.itviec

import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import reactivemongo.api.bson.{BSONDateTime, BSONDocument, BSONObjectID, document}
import reactivemongo.api.{Cursor, WriteConcern}

import vn.johu.messaging.RabbitMqClient
import vn.johu.persistence.MongoDb
import vn.johu.scraping.Scraper.{Command, ParseLocalRawJobSources, Scrape}
import vn.johu.scraping.jsoup.{HtmlDoc, HtmlElem, JSoup}
import vn.johu.scraping.models.ScrapedJobParsingField.ScrapedJobField
import vn.johu.scraping.models._
import vn.johu.scraping.{Scraper, models}
import vn.johu.utils.{Configs, DateUtils, Logging}

class ItViecScraper(
  context: ActorContext[Scraper.Command],
  jSoup: JSoup,
  timers: TimerScheduler[Scraper.Command]
) extends AbstractBehavior[Scraper.Command] with Logging {

  import ItViecScraper._

  implicit val ec: ExecutionContextExecutor = context.system.executionContext

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Scrape(page, replyTo) =>
        scrape(page, replyTo)
        this
      case ParseLocalRawJobSources(startTs, endTs) =>
        parseLocalRawJobSources(startTs, endTs)
        this
    }
  }

  private def parseLocalRawJobSources(startTs: Option[String], endTs: Option[String]) = {
    val parseResultF =
      findJobSources(startTs, endTs).flatMap { sources =>
        logger.info(s"Found ${sources.size} job sources.")
        Future.traverse(sources) { source =>
          val jobSourceId = source.getAsOpt[BSONObjectID](RawJobSource.Fields.id).get
          val htmlDoc = HtmlDoc.fromHtml(source.getAsOpt[String](RawJobSource.Fields.content).get)
          val parsingResult = parseJobsFromHtml(htmlDoc, jobSourceId)
          for {
            (existingJobs, newJobs) <- partitionExistingAndNewJobs(parsingResult.jobs)
            _ <- saveLocalParsingResults(existingJobs, newJobs, parsingResult.errors)
            _ <- publishJobs(existingJobs ++ newJobs)
          } yield {
            logger.info(
              s"""Parse job source [${jobSourceId.stringify}] result:
                 |nExistingJobs=${existingJobs.size}, nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}.
                 |""".stripMargin)
            (existingJobs.size, newJobs.size, parsingResult.errors.size)
          }
        }
      }

    parseResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when parsing from local job sources with params: startTs=$startTs, endTs=$endTs.", ex)
      case Success(results) =>
        val result = results.foldLeft((0, 0, 0))((r, elem) => (r._1 + elem._1, r._2 + elem._2, r._3 + elem._3))
        logger.info(
          s"""Success parsing from local job sources with params: startTs=$startTs, endTs=$endTs.
             |Result: nJobsUpdated=${result._1}, nJobsInserted=${result._2}, nErrors=${result._3}.
             |""".stripMargin
        )
    }

    parseResultF
  }

  private def findJobSources(startTs: Option[String], endTs: Option[String]) = {
    val startTimeOpt = startTs.map(DateUtils.parseDateTime)
    val endTimeOpt = endTs.map(DateUtils.parseDateTime)

    var query = document(RawJobSource.Fields.sourceName -> RawJobSourceName.ItViec.toString)
    startTimeOpt.foreach(t => query ++= RawJobSource.Fields.scrapingTs -> document("$gte" -> t))
    endTimeOpt.foreach(t => query ++= RawJobSource.Fields.scrapingTs -> document("$lte" -> t))

    val projection = document(RawJobSource.Fields.content -> 1)

    MongoDb.rawJobSourceColl.flatMap { coll =>
      coll.find(
        selector = query,
        projection = Some(projection),
      ).cursor[BSONDocument]().collect[List](-1, Cursor.FailOnError[List[BSONDocument]]())
    }
  }

  private def partitionExistingAndNewJobs(jobs: List[ScrapedJob]) = {
    for {
      newJobs <- filterNewJobs(jobs)
    } yield {
      val newJobKeys = newJobs.map(_.url).toSet
      val existingJobs = jobs.filterNot(j => newJobKeys.contains(j.url))
      (existingJobs, newJobs)
    }
  }

  private def saveLocalParsingResults(existingJobs: List[ScrapedJob], newJobs: List[ScrapedJob], errors: List[JobParsingError]) = {
    Future.sequence(
      List(
        updateJobs(existingJobs),
        insertJobs(newJobs),
        insertErrors(errors)
      )
    ).map(_ => ())
  }

  private def updateJobs(jobs: List[ScrapedJob]) = {
    if (jobs.isEmpty) {
      Future.successful(Nil)
    } else {
      val updateResultF = MongoDb.scrapedJobColl.flatMap { coll =>
        Future.traverse(jobs) { job =>
          val jobToUpdate = job.copy(id = None)
          coll.findAndUpdate(
            selector = document(ScrapedJob.Fields.url -> jobToUpdate.url),
            update = jobToUpdate,
            fetchNewObject = false,
            upsert = false,
            sort = None,
            fields = None,
            bypassDocumentValidation = false,
            writeConcern = WriteConcern.Acknowledged,
            maxTime = None,
            collation = None,
            arrayFilters = Seq.empty
          )
        }
      }
      updateResultF.map(_ => jobs)
    }
  }

  private def scrape(page: Int, replyTo: ActorRef[Scraper.JobsScraped]) = {
    val url = getPageUrl(page)
    logger.info(s"Start scraping at url: $url")

    // todo: how to handle when there's an error during the scraping?
    val scrapeResultF =
      for {
        htmlDoc <- getHtmlDoc(url)
        rawJobSourceId <- saveRawJobSource(url, htmlDoc)
        parsingResult <- Future.successful(parseJobsFromHtml(htmlDoc, rawJobSourceId))
        newJobs <- filterNewJobs(parsingResult.jobs)
        shouldScheduleNext <- Future.successful(shouldScheduleNextScraping(newJobs))
        _ <- saveParsingResults(newJobs, parsingResult.errors)
        _ <- publishJobs(newJobs)
        _ <- Future.successful(respond(page, newJobs, replyTo))
        _ <- Future.successful(scheduleNextScraping(shouldScheduleNext, page, replyTo))
      } yield {
        logger.info(s"Scrape result: nNewJobs=${newJobs.size}, nErrors=${parsingResult.errors.size}")
        parsingResult
      }

    scrapeResultF.onComplete {
      case Failure(ex) =>
        logger.error(s"Error when scraping from url: $url", ex)
      case Success(_) =>
        logger.info(s"Successfully scraped from url: $url")
    }

    scrapeResultF
  }

  private def getPageUrl(page: Int) = {
    s"$BaseUrl/it-jobs?page=$page"
  }

  private def getHtmlDoc(url: String) = {
    jSoup.getAsync(url)
  }

  private def parseJobsFromHtml(doc: HtmlDoc, rawJobSourceId: BSONObjectID) = {
    val jobElements = doc.select("#search-results #jobs div.job")
    if (jobElements.isEmpty) {
      ParsingResult(Nil, Nil)
    } else {
      val results = jobElements.map(parseJobElement(_, rawJobSourceId))
      ParsingResult(
        jobs = results.collect { case Right(value) => value },
        errors = results.collect { case Left(value) => value }
      )
    }
  }

  private def saveRawJobSource(url: String, htmlDoc: HtmlDoc): Future[BSONObjectID] = {
    MongoDb.rawJobSourceColl.flatMap { coll =>
      val docId = BSONObjectID.generate
      coll.insert(ordered = false).one[RawJobSource](
        RawJobSource(
          id = Some(docId),
          url = url,
          sourceName = RawJobSourceName.ItViec,
          content = htmlDoc.doc.outerHtml(),
          sourceType = RawJobSourceType.Html,
          scrapingTs = BSONDateTime(DateUtils.nowMillis())
        )
      ).map(_ => docId)
    }
  }

  // if successful, return ScrapedJob
  // if not, return JobParsingError(id, element html, all parsing errors, scrapeTs, rawJobSourceId)
  private def parseJobElement(jobElem: HtmlElem, rawJobSourceId: BSONObjectID): Either[JobParsingError, ScrapedJob] = {

    val parsingErrors = mutable.ListBuffer.empty[String]

    def trackError[T](key: ScrapedJobField)(f: => Option[T]) = {
      val opt = f
      if (opt.isEmpty) {
        val error = s"Missing ${key.toString}"
        parsingErrors += error
      }
      opt
    }

    def getUrl = trackError(ScrapedJobParsingField.url) {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
        link <- elem.attr("href")
      } yield s"$BaseUrl/${link.trim.stripPrefix("/")}"
    }

    def getTitle = trackError(ScrapedJobParsingField.title) {
      for {
        elem <- jobElem.selectFirst(".job_content > .job__description > .job__body > .details > .title > a")
      } yield elem.text().trim
    }

    def getTags = trackError(ScrapedJobParsingField.tags) {
      for {
        elems <- jobElem.selectOpt(".job_content > .job__description > .job-bottom > .tag-list > a > span")
      } yield elems.map(_.text().trim).toSet
    }

    def parsePostingDate(dateStr: String): Option[BSONDateTime] = {
      import PostingDatePatterns._
      val parsedDate = dateStr.trim.toLowerCase match {
        case Second(v, _) => Some((v.toInt, ChronoUnit.SECONDS))
        case Minute(v, _) => Some((v.toInt, ChronoUnit.MINUTES))
        case Hour(v, _) => Some((v.toInt, ChronoUnit.HOURS))
        case Day(v, _) => Some((v.toInt, ChronoUnit.DAYS))
        case Week(v, _) => Some((v.toInt, ChronoUnit.WEEKS))
        case Month(v, _) => Some((v.toInt, ChronoUnit.MONTHS))
        case _ => None
      }
      parsedDate.map { pd =>
        val currentTime = DateUtils.now()
        BSONDateTime(DateUtils.toMillis(currentTime.minus(pd._1, pd._2)))
      }
    }

    def getPostingDate = trackError(ScrapedJobParsingField.postingDate) {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .distance-time-job-posted > span")
        d <- parsePostingDate(elem.text())
      } yield d
    }

    def getCompany = trackError(ScrapedJobParsingField.company) {
      for {
        elem <- jobElem.selectFirst(".job_content > .logo > .logo-wrapper > a")
        link <- elem.attr("href")
      } yield link.stripPrefix("/companies/").trim
    }

    def getLocation = trackError(ScrapedJobParsingField.location) {
      for {
        elem <- jobElem.selectFirst(".job_content > .city_and_posted_date > .city > .address > .text")
      } yield elem.text().trim
    }

    val jobOpt =
      for {
        id <- Some(BSONObjectID.generate())
        url <- getUrl
        title <- getTitle
        tags <- getTags
        postingDate <- getPostingDate
        company <- getCompany
        location <- getLocation
      } yield {
        ScrapedJob(
          id = Some(id),
          url = url,
          title = title,
          tags = tags,
          postingDate = postingDate,
          company = company,
          location = location,
          rawJobSourceName = RawJobSourceName.ItViec,
          rawJobSourceId = rawJobSourceId
        )
      }

    jobOpt match {
      case Some(job) =>
        Right(job)
      case None =>
        val id = BSONObjectID.generate()
        logger.error(s"Could not parse job element. One of the required field is missing. Error id: [${id.stringify}]")
        Left(
          models.JobParsingError(
            id = id,
            rawSourceContent = jobElem.elem.outerHtml(),
            errors = parsingErrors.toList,
            rawJobSourceId = rawJobSourceId,
            scrapingTs = BSONDateTime(DateUtils.nowMillis())
          )
        )
    }
  }

  private def saveParsingResults(newJobs: List[ScrapedJob], errors: List[JobParsingError]) = {
    val insertJobsF = insertJobs(newJobs)
    val insertErrorsF = insertErrors(errors)
    for {
      res1 <- insertJobsF
      res2 <- insertErrorsF
    } yield (res1, res2)
  }

  private def insertJobs(jobs: List[ScrapedJob]) = {
    if (jobs.isEmpty) {
      Future.successful(Nil)
    } else {
      MongoDb.scrapedJobColl.flatMap { coll =>
        coll.insert(ordered = false).many(jobs).map(_ => jobs)
      }
    }
  }

  private def insertErrors(errors: List[JobParsingError]) = {
    if (errors.isEmpty) {
      Future.successful(Nil)
    } else {
      MongoDb.jobParsingErrorColl.flatMap { coll =>
        coll.insert(ordered = false).many(errors).map(_ => errors)
      }
    }
  }

  private def publishJobs(jobs: List[ScrapedJob]) = {
    Future.traverse(jobs) { job =>
      RabbitMqClient.publishAsync(job)
    }
  }

  private def filterNewJobs(scrapedJobs: List[ScrapedJob]) = {
    if (scrapedJobs.isEmpty) {
      Future.successful(Nil)
    } else {
      val jobByKey = scrapedJobs.map(j => j.url -> j).toMap
      val jobKeys = jobByKey.keySet
      val existingJobsF = MongoDb.scrapedJobColl.flatMap { coll =>
        coll.find(
          selector = document(
            ScrapedJob.Fields.url -> document("$in" -> jobKeys),
            ScrapedJob.Fields.rawJobSourceName -> RawJobSourceName.ItViec.toString
          ),
          projection = Some(document(ScrapedJob.Fields.url -> 1))
        ).cursor[BSONDocument]().collect[List](jobKeys.size, Cursor.FailOnError[List[BSONDocument]]())
      }

      for {
        existingJobs <- existingJobsF
      } yield {
        val existingJobKeys = existingJobs.map(_.getAsOpt[String](ScrapedJob.Fields.url).get).toSet
        val newJobs = jobByKey.filterNot(kv => existingJobKeys.contains(kv._1)).values.toList
        newJobs
      }
    }
  }

  private def shouldScheduleNextScraping(newJobs: List[ScrapedJob]) = newJobs.nonEmpty

  private def respond(page: Int, jobs: List[ScrapedJob], replyTo: ActorRef[Scraper.JobsScraped]): Unit = {
    replyTo ! Scraper.JobsScraped(page, jobs)
  }

  private def scheduleNextScraping(
    shouldSchedule: Boolean,
    currentPage: Int,
    replyTo: ActorRef[Scraper.JobsScraped]
  ): Unit = {
    if (!shouldSchedule) {
      logger.info("Not schedule for next scraping.")
    } else {
      val nextPage = currentPage + 1
      val config = context.system.settings.config
      val delay = config.getLong(Configs.ScrapingDelayInMillis)
      timers.startSingleTimer(
        ContinueScraping,
        Scrape(nextPage, replyTo),
        delay = FiniteDuration(delay, TimeUnit.MILLISECONDS)
      )
      logger.info(s"Scheduled for the next scraping: currentPage=$currentPage, nextPage=$nextPage...")
    }
  }

}

object ItViecScraper {

  val BaseUrl = "https://itviec.com"

  def apply(jSoup: JSoup): Behavior[Scraper.Command] = {
    Behaviors.setup[Scraper.Command] { ctx =>
      Behaviors.withTimers[Scraper.Command] { timers =>
        new ItViecScraper(ctx, jSoup, timers)
      }
    }
  }

  object PostingDatePatterns {
    val Second: Regex = "^(\\d+)\\s*(second|seconds)\\s*ago$".r
    val Minute: Regex = "^(\\d+)\\s*(minute|minutes)\\s*ago$".r
    val Hour: Regex = "^(\\d+)\\s*(hour|hours)\\s*ago$".r
    val Day: Regex = "^(\\d+)\\s*(day|days)\\s*ago$".r
    val Week: Regex = "^(\\d+)\\s*(week|weeks)\\s*ago$".r
    val Month: Regex = "^(\\d+)\\s*(month|months)\\s*ago$".r
  }

  case object ContinueScraping

}

case class ParsingResult(jobs: List[ScrapedJob], errors: List[JobParsingError])