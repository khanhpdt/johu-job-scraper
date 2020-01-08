package vn.johu.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.util.{Failure, Success, Try}

import reactivemongo.api.bson.BSONDateTime

object DateUtils extends Logging {

  private val dtf = DateTimeFormatter.ISO_DATE_TIME

  def now(): LocalDateTime = LocalDateTime.now()

  def nowMillis(): Long = toMillis(now())

  def toMillis(dt: LocalDateTime): Long = dt.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

  def tryParseDateTime(s: String): Try[LocalDateTime] = Try(LocalDateTime.parse(s, dtf))

  def parseDateTime(s: String): LocalDateTime = {
    tryParseDateTime(s) match {
      case Success(value) =>
        value
      case Failure(ex) =>
        logger.error(s"Invalid date time format: $s. Expect ISO format.")
        throw ex
    }
  }

  def toDateTime(d1: BSONDateTime): LocalDateTime = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(d1.value), ZoneId.systemDefault())
  }

  def isSameDate(d1: BSONDateTime, d2: BSONDateTime): Boolean = {
    toDateTime(d1).toLocalDate.isEqual(toDateTime(d2).toLocalDate)
  }

}
