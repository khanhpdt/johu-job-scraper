package vn.johu.utils

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

object DateUtils {

  def now(): ZonedDateTime = {
    LocalDateTime.now().atZone(ZoneId.systemDefault())
  }

  def nowMillis(): Long = {
    now().toInstant.toEpochMilli
  }

}
