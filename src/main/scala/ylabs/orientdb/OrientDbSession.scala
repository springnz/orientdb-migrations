package ylabs.orientdb

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

class OrientDbSession[+A](val block: ODatabaseDocumentTx ⇒ A) {

  def run()(implicit dbConnectionPool: OrientDocumentDBConnectionPool): Try[A] =
    dbConnectionPool
      .acquire()
      .flatMap(db ⇒ Try(run(db))
        .recoverWith {
          case e ⇒
            closeIfOpen(db)
            Failure(e)
        }) // todo: make nicer

  private def run(db: ODatabaseDocumentTx): A = {
    //db.setCurrentDatabaseInThreadLocal()
    ODatabaseRecordThreadLocal.INSTANCE.set(db)
    val result = block(db)
    closeIfOpen(db)
    result
  }

  def runAsync()(implicit dbConnectionPool: OrientDocumentDBConnectionPool, ec: ExecutionContext): Future[A] = Future[A] {
    run() match {
      case Success(x) ⇒ x
      case Failure(e) ⇒ throw e
    }
  }

  // TODO more tests on cancellation
  def runAsyncCancellable()(implicit dbConnectionPool: OrientDocumentDBConnectionPool, ec: ExecutionContext): (() ⇒ Unit, Future[A]) = {
    val promise = Promise[A]()

    //TODO cleanup, make more comprehendable
    val future = Future[A] {
      val tried = dbConnectionPool.acquire().flatMap { db ⇒
        // closing will happen on different thread (promise), but orient seems to be ok with it
        promise.future.onFailure { case e ⇒ closeIfOpen(db) }
        Try(run(db)).recoverWith {
          case e ⇒
            closeIfOpen(db)
            Failure(e)
        }
      }

      tried match {
        case Success(x) ⇒ x
        case Failure(e) ⇒ throw e
      }
    }

    val cancellation = () ⇒ {
      promise.failure(new Exception(s"${Thread.currentThread().getId} canceled"))
      ()
    }

    (cancellation, Future.firstCompletedOf(Seq(promise.future, future)))
  }

  private def closeIfOpen(db: ODatabaseDocumentTx): Unit =
    if (!db.isClosed) db.close()

  // todo scalaZ ? todo verify ?
  def map[B](fn: A ⇒ B): OrientDbSession[B] =
    flatMap(a ⇒ new OrientDbSession[B](db ⇒ fn(a)))

  def flatMap[B](fn: A ⇒ OrientDbSession[B]): OrientDbSession[B] = {
    new OrientDbSession[B](db ⇒ fn(block(db)).block(db))
  }
}

object OrientDbSession {
  def apply[A](block: ODatabaseDocumentTx ⇒ A): OrientDbSession[A] = new OrientDbSession[A](block)
  def map2[A, B, C](a: OrientDbSession[A], b: OrientDbSession[B])(f: (A, B) ⇒ C): OrientDbSession[C] = ??? // todo...just use scalaz
}
