package ylabs.orientdb

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Try }

final case class ODBSession[+A](block: ODatabaseDocumentTx ⇒ A) {

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
}

import scalaz._
object ODBSession {
  implicit val monad = new Monad[ODBSession] {
    override def point[A](a: => A): ODBSession[A] =
      ODBSession[A](db => a)
    override def bind[A, B](fa: ODBSession[A])(f: (A) => ODBSession[B]): ODBSession[B] =
      ODBSession[B](db ⇒ f(fa.block(db)).block(db))
  }
}
