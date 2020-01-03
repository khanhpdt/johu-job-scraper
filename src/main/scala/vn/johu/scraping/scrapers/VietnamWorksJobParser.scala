package vn.johu.scraping.scrapers

import io.circe.parser.parse
import io.circe.{Decoder, DecodingFailure, Json}
import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.scraping.models.{JobParsingError, RawJobSource, ScrapedJob}

object VietnamWorksJobParser extends JobParser {

  override def parseJobs(rawJobSource: RawJobSource): JobParsingResult = {
    parse(rawJobSource.content) match {
      case Left(failure) =>
        JobParsingResult(
          jobs = Nil,
          errors = List(JobParsingError(
            id = BSONObjectID.generate,
            rawSourceContent = "",
            errors = List(s"Cannot parse job source as json. Failure message: ${failure.message}"),
            rawJobSourceId = rawJobSource.id.get
          ))
        )
      case Right(json) =>
        json.hcursor.downField("results").downArray.downField("hits").values match {
          case Some(elementsJson) if elementsJson.nonEmpty =>
            val results = elementsJson.toList.map(parseJobElement(_, rawJobSource))
            JobParsingResult(
              jobs = results.collect { case Right(value) => value },
              errors = results.collect { case Left(value) => value }
            )
          case _ =>
            JobParsingResult(Nil, Nil)
        }
    }

  }

  private def parseJobElement(jobElement: Json, rawJobSource: RawJobSource): Either[JobParsingError, ScrapedJob] = {
    import VietnamWorksJobScraper.BaseUrl

    def get[T: Decoder](key: String) = jobElement.hcursor.get[T](key)

    import scala.language.implicitConversions
    implicit def converter[T](e: Either[DecodingFailure, T]): Option[T] = e.toOption

    def getUrl = {
      for {
        alias <- get[String]("alias")
        jobId <- get[Long]("jobId")
      } yield s"$BaseUrl/${alias.trim}-$jobId-jv"
    }

    def getTitle = {
      for {
        title <- get[String]("jobTitle")
      } yield title.trim
    }

    def getTags = {
      for {
        skills <- get[Set[String]]("skills")
      } yield skills.map(_.trim)
    }

    def getPostingDate = {
      for {
        onlineDate <- get[Long]("onlineDate")
      } yield BSONDateTime(onlineDate * 1000)
    }

    def getCompany = {
      for {
        company <- get[String]("company")
      } yield company.trim
    }

    def getLocations = {
      for {
        locations <- get[Set[String]]("locations")
      } yield locations.map(_.trim)
    }

    buildScrapedJob(
      url = getUrl,
      title = getTitle,
      tags = getTags,
      postingDate = getPostingDate,
      company = getCompany,
      locations = getLocations,
      rawJobSource = rawJobSource,
      rawJobElementSource = jobElement.toString
    )
  }

}
