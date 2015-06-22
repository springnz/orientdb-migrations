package ylabs.util

import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

object Pimpers {

  implicit class TryPimper[A](t: Try[A]) {
    def withErrorLog(msg: String)(implicit log: Logger) =
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
