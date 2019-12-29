package vn.johu.scraping.scrapers

import io.circe.Json
import io.circe.parser.parse
import reactivemongo.api.bson.BSONObjectID

import vn.johu.scraping.models.{JobParsingError, RawJobSource, ScrapedJob}

object VietnamWorksParser extends Parser {

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

  }

}
