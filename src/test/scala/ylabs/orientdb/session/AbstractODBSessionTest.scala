package ylabs.orientdb.session

import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures._
import ylabs.orientdb.ODBScala
import ylabs.orientdb.pool.AbstractODBConnectionPool
import ylabs.util.Logging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import scalaz.std.list.listInstance
import scalaz.syntax.bind._

class AbstractODBSessionTest extends WordSpec with BeforeAndAfterEach with BeforeAndAfterAll with ODBScala with Logging {

  final case class TestSession[+A](override val block: Unit ⇒ A) extends AbstractODBSession[A, Unit](block) {
    def run(db: Unit): A = block()
    def close(db: Unit): Unit = ()
  }

  object TestSession extends ODBSessionInstances[Unit, TestSession]

  val random = scala.util.Random
  implicit val ec = ExecutionContext.global

  implicit val pool = new AbstractODBConnectionPool[Unit] {
    def acquire() = Try { Unit }
  }

  val monad = TestSession.monad

  "map" should {
    "work in simple case" in {
      val session = TestSession { _ ⇒ 1 }
      val mapped = session.map(_ * 2)
      val future = mapped.runAsync()(pool, ec)

      whenReady(future, Timeout(3.seconds)) { f ⇒
        f shouldBe 2
      }
    }
  }

  "flatMap" should {
    "work properly" in {
      val start = TestSession { _ ⇒ 1 }

      def doSomething(x: Int) = TestSession { _ ⇒ x + 10 }

      val session = for {
        x ← start
        y ← doSomething(x)
      } yield y

      whenReady(session.runAsync()(pool, ec), Timeout(3.seconds)) { f ⇒
        f shouldBe 11
      }
    }

    // todo: scalacheck ? we verify that it's monad only for number 5 :-)
    "have left identity" in {
      val number = random.nextInt()
      val unit: Int ⇒ TestSession[Int] = x ⇒ TestSession { _ ⇒ x }
      val f: Int ⇒ TestSession[Int] = z ⇒ TestSession { _ ⇒ z * 2 }

      val left = unit(number).flatMap(f)
      val right = f(number)

      whenReady(left.runAsync()(pool, ec), Timeout(3.seconds)) { leftVal ⇒
        whenReady(right.runAsync()(pool, ec), Timeout(3.seconds)) { rightVal ⇒
          leftVal shouldBe rightVal
        }
      }
    }

    "have right identity" in {
      val number = random.nextInt()
      val unit: Int ⇒ TestSession[Int] = x ⇒ TestSession { _ ⇒ x }

      val m = TestSession { _ ⇒ number }
      val left = m.flatMap(unit)
      val right = m

      whenReady(left.runAsync()(pool, ec), Timeout(3.seconds)) { leftVal ⇒
        whenReady(right.runAsync()(pool, ec), Timeout(3.seconds)) { rightVal ⇒
          leftVal shouldBe rightVal
        }
      }
    }

    "be associative" in {
      val number = random.nextInt(10000)
      val f: Int ⇒ TestSession[Int] = z ⇒ TestSession { _ ⇒ z * 2 }
      val g: Int ⇒ TestSession[Int] = z ⇒ TestSession { _ ⇒ z - 100 }
      val m = TestSession { _ ⇒ number }

      val left = m.flatMap(f).flatMap(g)
      val right = m.flatMap(x ⇒ f(x).flatMap(g))

      whenReady(left.runAsync()(pool, ec), Timeout(3.seconds)) { leftVal ⇒
        whenReady(right.runAsync()(pool, ec), Timeout(3.seconds)) { rightVal ⇒
          leftVal shouldBe rightVal
        }
      }
    }

    "not change order of execution" in {
      var list = List.empty[Int]

      val session = for {
        x ← TestSession { _ ⇒ list = 3 :: list }
        y ← TestSession { _ ⇒ list = 2 :: list }
        z ← TestSession { _ ⇒ list = 1 :: list }
      } yield ()

      whenReady(session.runAsync()(pool, ec), Timeout(3.seconds)) { f ⇒
        list shouldBe List(1, 2, 3)
      }
    }
  }

  "runAsync" should {
    "starts one thread for run of one combined instance" in {
      var startedThreads = 0
      val testEc: ExecutionContext = new ExecutionContext {
        // mutate global state, but from same thread, since this execution context won't create any more
        override def execute(runnable: Runnable): Unit = startedThreads += 1

        override def reportFailure(cause: Throwable): Unit = throw new Exception("Shouldn't happen")
      }

      val session = for {
        x ← TestSession { _ ⇒ 1 }
        y ← TestSession { _ ⇒ 1 }
      } yield y

      session.runAsync()(pool, testEc)

      startedThreads shouldBe 1
    }

    "starts two threads for two sessions" in {
      var startedThreads = 0
      val testEc: ExecutionContext = new ExecutionContext {
        // mutate global state, but from same thread, since this execution context won't create any more
        override def execute(runnable: Runnable): Unit = startedThreads += 1

        override def reportFailure(cause: Throwable): Unit = throw new Exception("Shouldn't happen")
      }

      val session = TestSession { _ ⇒ () }
      session.runAsync()(pool, testEc)
      session.runAsync()(pool, testEc)

      startedThreads shouldBe 2
    }

    "correctly propagates exception to future" in {
      val exceptionThrowingQuery = TestSession { db ⇒
        throw new IllegalStateException()
      }

      val future = exceptionThrowingQuery.runAsync()(pool, ec)

      whenReady(future.failed, Timeout(3.seconds)) { e ⇒
        e shouldBe a[IllegalStateException]
      }
    }
  }

  "runAsyncCancellable" should {
    "enable cancellation of async session" in {
      val exceptionThrowingQuery = TestSession { db ⇒
        throw new IllegalStateException()
        123 // force type to be Int instead of Nothing
      }

      val session = for {
        x ← TestSession { _ ⇒ Thread.sleep(5000) }
        y ← TestSession { _ ⇒ Thread.sleep(5000) }
      } yield y

      val (cancellation, result) = session.runAsyncCancellable()(pool, ec)
      Thread.sleep(200)
      cancellation()

      whenReady(result.failed, Timeout(6.seconds)) { f ⇒
        f should not be a[IllegalStateException]
        f shouldBe a[Exception]
      }
    }
  }

  "sequence" should {
    "convert a List of TestSessions into a TestSession of List" in {
      val values = List(1, 3, 2)
      val sessions = values.map(value ⇒ TestSession(_ ⇒ value))
      monad.sequence(sessions).run().get shouldBe values
    }
  }
}

