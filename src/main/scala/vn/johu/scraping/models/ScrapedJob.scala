package vn.johu.scraping.models

import java.util.Date

import reactivemongo.api.bson.BSONObjectID

case class ScrapedJob(
  id: Option[BSONObjectID],
  title: String,
  keywords: Set[String],
  postingDate: Date,
  company: String,
  location: String,
  rawHtmlId: BSONObjectID
)
