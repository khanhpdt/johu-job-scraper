package vn.johu.scraping.scrapers

import scala.collection.mutable

import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

import vn.johu.scraping.models.{JobParsingError, RawJobSource, ScrapedJob}
import vn.johu.utils.Logging

trait JobParser extends Logging {

  def parseJobs(rawJobSource: RawJobSource): JobParsingResult

  protected def buildScrapedJob(
    url: Option[String],
    title: Option[String],
    tags: Option[Set[String]],
    postingDate: Option[BSONDateTime],
    company: Option[String],
    locations: Option[Set[String]],
    rawJobSource: RawJobSource,
    rawJobElementSource: String
  ): Either[JobParsingError, ScrapedJob] = {
    var errors = mutable.ListBuffer.empty[String]

    if (url.isEmpty) errors += "Missing url"
    if (title.isEmpty) errors += "Missing title"
    if (tags.forall(_.isEmpty)) errors += "Missing tags"
    if (postingDate.isEmpty) errors += "Missing posting date"
    if (company.isEmpty) errors += "Missing company"
    if (locations.forall(_.isEmpty)) errors += "Missing locations"

    if (errors.nonEmpty) {
      val id = BSONObjectID.generate()
      logger.error(s"Could not parse job element. One of the required field is missing. Error id: [${id.stringify}]")
      Left(JobParsingError(
        id = id,
        rawSourceContent = rawJobElementSource,
        errors = errors.toList,
        rawJobSourceId = rawJobSource.id.get
      ))
    } else {
      Right(ScrapedJob(
        id = Some(BSONObjectID.generate()),
        url = url.get,
        title = title.get,
        tags = tags.get,
        postingDate = postingDate.get,
        company = company.get,
        locations = locations.get,
        rawJobSourceName = rawJobSource.sourceName,
        rawJobSourceId = rawJobSource.id.get
      ))
    }
  }

}

case class JobParsingResult(jobs: List[ScrapedJob], errors: List[JobParsingError])