package springnz.orientdb.util

import java.time._
import java.util.Date

import com.typesafe.scalalogging.Logger
import org.joda.time.DateTime

import scala.util.{Failure, Try}

object Pimpers {

  implicit class TryPimper[A](t: Try[A]) {
    def withErrorLog(msg: String)(implicit log: Logger): Try[A] =
      t.recoverWith {
        case e ⇒
          log.error(msg, e)
          Failure(e)
      }

    def withFinally[T](block: ⇒ T): Try[A] = {
      block
      t
    }
  }
  
  implicit class OffsetDateTimePimper(offsetDateTime: OffsetDateTime) {
    def toLegacyDate: Date = Date.from(offsetDateTime.toInstant)
    def toJodaTime: DateTime = DateTime.parse(offsetDateTime.toString)
  }

  implicit class ZonedDateTimePimper(zonedDateTime: ZonedDateTime) {
    def toLegacyDate: Date = Date.from(zonedDateTime.toInstant)
  }
}
