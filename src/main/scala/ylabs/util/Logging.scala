package ylabs.util

import org.slf4j.LoggerFactory

trait Logging {
  implicit val log = LoggerFactory.getLogger(this.getClass)
}
