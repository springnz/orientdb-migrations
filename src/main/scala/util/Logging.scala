package springnz.orientdb.util

import com.typesafe.scalalogging.{LazyLogging, Logger}

trait Logging extends LazyLogging {
  implicit lazy val log: Logger = logger
}
