package ylabs.orientdb.session

import ylabs.orientdb.pool.AbstractODBConnectionPool

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Try }
import scalaz.Monad

abstract class AbstractODBSession[+A, Database](val block: Database ⇒ A) {

  def run[Pool <: AbstractODBConnectionPool[Database]]()(implicit pool: Pool): Try[A] =
    pool
      .acquire()
      .flatMap(graph ⇒ Try(run(graph))
        .recoverWith {
          case e ⇒
            close(graph)
            Failure(e)
        })

  def run(database: Database): A

  def runAsync()(implicit pool: AbstractODBConnectionPool[Database], ec: ExecutionContext): Future[A] =
    Future[A] {
      run().get
    }

  // TODO more tests on cancellation
  def runAsyncCancellable()(implicit pool: AbstractODBConnectionPool[Database], ec: ExecutionContext): (() ⇒ Unit, Future[A]) = {
    val promise = Promise[A]()

    val future = Future[A] {
      val tried = pool.acquire().flatMap { database ⇒
        // closing will happen on different thread (promise), but orient seems to be ok with it
        promise.future.onFailure {
          case e ⇒ close(database)
        }

        Try(run(database)).recoverWith {
          case e ⇒
            close(database)
            Failure(e)
        }
      }
      tried.get
    }

    def cancellation(): Unit =
      promise.failure(new Exception(s"${Thread.currentThread().getId} canceled"))

    (cancellation, Future.firstCompletedOf(Seq(promise.future, future)))
  }

  def close(database: Database): Unit
}

abstract class ODBSessionInstances[Database, Session[A] <: AbstractODBSession[A, Database]] {

  implicit def monad[T] = new Monad[Session] {
    override def point[A](a: ⇒ A): Session[A] =
      ODBSessionInstances.this.apply[A](db ⇒ a)
    override def bind[A, B](fa: Session[A])(f: (A) ⇒ Session[B]): Session[B] =
      ODBSessionInstances.this.apply[B](db ⇒ f(fa.block(db)).block(db))
  }

  def apply[A](block: Database ⇒ A): Session[A]
}
