package ylabs.logging

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

trait Logging {
  val log = LoggerFactory.getLogger(this.getClass)

  implicit class TryPimper[A](t: Try[A]) {
    def withErrorLog(msg: String) =
      t.recoverWith {
        case e ⇒
          log.error(msg, e)
          Failure(e)
      }

    def withFinally[T](block: ⇒ T) =
      t match {
        case Success(result) ⇒
          block
          Success(result)
        case Failure(e) ⇒
          block
          Failure(e)
      }
  }
}
