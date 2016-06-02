package springnz.orientdb.util

import java.time._

object DateTimeUtil {
  lazy val UTCTimeZone: ZoneId = ZoneId.of("UTC")
  lazy val NZTimeZone: ZoneId = ZoneId.of("Pacific/Auckland")

  def utcOffsetDateTime: OffsetDateTime = OffsetDateTime.now(UTCTimeZone)

  def utcZonedDateTime: ZonedDateTime = ZonedDateTime.now(UTCTimeZone)
}
