package ylabs.orientdb

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Try }

class ODBSession[+A](val block: ODatabaseDocumentTx ⇒ A) {

  def run()(implicit dbConnectionPool: ODBConnectionPool): Try[A] =
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

  def runAsync()(implicit dbConnectionPool: ODBConnectionPool, ec: ExecutionContext): Future[A] =
    Future[A] {
      run().get
    }

  // TODO more tests on cancellation
  def runAsyncCancellable()(implicit dbConnectionPool: ODBConnectionPool, ec: ExecutionContext): (() ⇒ Unit, Future[A]) = {
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
      tried.get
    }

    def cancellation(): Unit =
      promise.failure(new Exception(s"${Thread.currentThread().getId} canceled"))

    (cancellation, Future.firstCompletedOf(Seq(promise.future, future)))
  }

  private def closeIfOpen(db: ODatabaseDocumentTx): Unit =
    if (!db.isClosed) db.close()

  // todo scalaZ ? todo verify ?
  def map[B](fn: A ⇒ B): ODBSession[B] =
    flatMap(a ⇒ new ODBSession[B](db ⇒ fn(a)))

  def flatMap[B](fn: A ⇒ ODBSession[B]): ODBSession[B] = {
    new ODBSession[B](db ⇒ fn(block(db)).block(db))
  }

}

object ODBSession {
  def apply[A](block: ODatabaseDocumentTx ⇒ A): ODBSession[A] = new ODBSession[A](block)

  def map2[A, B, C](a: ODBSession[A], b: ODBSession[B])(f: (A, B) ⇒ C): ODBSession[C] = ??? // todo...just use scalaz

  def sequence[A](sessions: Seq[ODBSession[A]]): ODBSession[Seq[A]] =
    sessions.foldLeft(ODBSession(_ ⇒ Seq.empty[A])) {
      case (a, sess) ⇒ a.flatMap(acc ⇒ sess.map(acc :+ _))
    }
}
