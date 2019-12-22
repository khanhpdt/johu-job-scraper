package vn.johu.scraping.models

import reactivemongo.api.bson.{BSONDateTime, BSONObjectID}

case class ScrapedJob(
  id: Option[BSONObjectID],
  url: String,
  title: String,
  tags: Set[String],
  postingDate: BSONDateTime,
  company: String,
  location: String,
  rawJobSourceId: BSONObjectID
)
