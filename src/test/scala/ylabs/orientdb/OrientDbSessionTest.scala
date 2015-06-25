package ylabs.orientdb

import com.orientechnologies.orient.core.exception.ODatabaseException
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Try, _}

class OrientDbSessionTest extends WordSpec with BeforeAndAfterEach with BeforeAndAfterAll with OrientDocumentDBScala {

  val random = scala.util.Random
  implicit val ec = ExecutionContext.global

  "map" should {
    "work in simple case" in new Fixture {
      val session = OrientDbSession { _ ⇒ 1 }
      val mapped = session.map(_ * 2)
      val future = mapped.runAsync()(pool, ec)

      whenReady(future, Timeout(3.seconds)) { f ⇒
        f shouldBe 2
      }
    }


    "work with db" in new Fixture {
      Await.result(createDbAndUsers.runAsync()(pool, ec), 3.seconds)
      whenReady(countUsersSession.runAsync()(pool, ec), Timeout(3.seconds)) { notMapped ⇒
        val mapped = countUsersSession.map(_ * 3)
        whenReady(mapped.runAsync()(pool, ec), Timeout(3.seconds)) { mapped ⇒
          mapped shouldBe (notMapped * 3)
          Await.result(cleanupUsersAndDb.runAsync()(pool, ec), 3.seconds)
        }
      }
    }
  }

  "flatMap" should {
  "work properly" in new Fixture {
      val start = OrientDbSession { _ ⇒ 1 }

      def doSomething(x: Int) = OrientDbSession { _ ⇒ x + 10 }

      val session = for {
        x ← start
        y ← doSomething(x)
      } yield y

      whenReady(session.runAsync()(pool, ec), Timeout(3.seconds)) { f ⇒
        f shouldBe 11
      }
    }

    // todo: scalacheck ? we verify that it's monad only for number 5 :-)
    "have left identity" in new Fixture {
      val number = random.nextInt
      val unit: Int ⇒ OrientDbSession[Int] = x ⇒ OrientDbSession { _ ⇒ x }
      val f: Int ⇒ OrientDbSession[Int] = z ⇒ OrientDbSession { _ ⇒ z * 2 }

      val left = unit(number).flatMap(f)
      val right = f(number)

      whenReady(left.runAsync()(pool, ec), Timeout(3.seconds)) { leftVal ⇒
        whenReady(right.runAsync()(pool, ec), Timeout(3.seconds)) { rightVal ⇒
          leftVal shouldBe rightVal
        }
      }
    }

    "have right identity" in new Fixture {
      val number = random.nextInt
      val unit: Int ⇒ OrientDbSession[Int] = x ⇒ OrientDbSession { _ ⇒ x }

      val m = OrientDbSession { _ ⇒ number }
      val left = m.flatMap(unit)
      val right = m

      whenReady(left.runAsync()(pool, ec), Timeout(3.seconds)) { leftVal ⇒
        whenReady(right.runAsync()(pool, ec), Timeout(3.seconds)) { rightVal ⇒
          leftVal shouldBe rightVal
        }
      }
    }

    "be associative" in new Fixture {
      val number = random.nextInt(10000)
      val f: Int ⇒ OrientDbSession[Int] = z ⇒ OrientDbSession { _ ⇒ z * 2 }
      val g: Int ⇒ OrientDbSession[Int] = z ⇒ OrientDbSession { _ ⇒ z - 100 }
      val m = OrientDbSession { _ ⇒ number }

      val left = m.flatMap(f).flatMap(g)
      val right = m.flatMap(x ⇒ f(x).flatMap(g))

      whenReady(left.runAsync()(pool, ec), Timeout(3.seconds)) { leftVal ⇒
        whenReady(right.runAsync()(pool, ec), Timeout(3.seconds)) { rightVal ⇒
          leftVal shouldBe rightVal
        }
      }
    }

    "work in for comprehension" in new Fixture {
      val session = for {
        x ← createDbAndUsers
        y ← countUsersSession
        _ ← cleanupUsersAndDb
      } yield y

      val result = session.runAsync()(pool, ec)

      whenReady(result, Timeout(3.seconds)) { f ⇒
        f shouldBe userCount
      }
    }

    "closing session work" in new Fixture {
      val killSession = OrientDbSession { _.close() }

      val session = for {
        x ← createDbAndUsers
        _ ← killSession
        y ← countUsersSession
        _ ← cleanupUsersAndDb
      } yield y

      val result = session.runAsync()(pool, ec)

      whenReady(result.failed, Timeout(3.seconds)) { f ⇒
        f shouldBe a[ODatabaseException]
      }
    }

    "not change order of execution" in new Fixture {
      var list = List.empty[Int]

      val session = for {
        x ← OrientDbSession { _ ⇒ list = 3 :: list }
        y ← OrientDbSession { _ ⇒ list = 2 :: list }
        z ← OrientDbSession { _ ⇒ list = 1 :: list }
      } yield ()

      whenReady(session.runAsync()(pool, ec), Timeout(3.seconds)) { f ⇒
        list shouldBe List(1, 2, 3)
      }
    }
  }

  "runAsync" should {
    "starts one thread for run of one combined instance" in new Fixture {
      var startedThreads = 0
      val testEc: ExecutionContext = new ExecutionContext {
        // mutate global state, but from same thread, since this execution context won't create any more
        override def execute(runnable: Runnable): Unit = startedThreads += 1

        override def reportFailure(cause: Throwable): Unit = throw new Exception("Shouldn't happen")
      }

      val session = for {
        x ← createDbAndUsers
        y ← countUsersSession
        _ ← cleanupUsersAndDb
      } yield y

      session.runAsync()(pool, testEc)

      startedThreads shouldBe 1
    }

    "starts two threads for two sessions" in new Fixture {
      var startedThreads = 0
      val testEc: ExecutionContext = new ExecutionContext {
        // mutate global state, but from same thread, since this execution context won't create any more
        override def execute(runnable: Runnable): Unit = startedThreads += 1

        override def reportFailure(cause: Throwable): Unit = throw new Exception("Shouldn't happen")
      }

      (for {
        x ← createDbAndUsers
        y ← countUsersSession
      } yield y).runAsync()(pool, testEc)
      val f = cleanupUsersAndDb.runAsync()(pool, testEc)

      startedThreads shouldBe 2
    }

    "correctly propagates exception to future" in new Fixture {
      val exceptionThrowingQuery = OrientDbSession { db ⇒
        throw new IllegalStateException()
      }

      val future = exceptionThrowingQuery.runAsync()(pool, ec)

      whenReady(future.failed, Timeout(3.seconds)) { e ⇒
        e shouldBe a[IllegalStateException]
      }
    }
  }

  "runAsyncCancellable" should {
    "enable us cancelling future" in new Fixture {
      val exceptionThrowingQuery = OrientDbSession { db ⇒
        throw new IllegalStateException()
      }

      val session = for {
        x ← longRunningQuery
        y ← countUsersSession
        _ ← exceptionThrowingQuery
        _ ← cleanupUsersAndDb
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

  trait Fixture {
    val userCount = 10

    implicit val pool = new OrientDocumentDBConnectionPool {
      override def loadDBConfig: Try[DBConfig] = Success(DBConfig("memory:doctest-session", "admin", "admin"))
    }


    val createDbAndUsers = OrientDbSession { implicit db ⇒
      db.create()
      createClass("User").createProperty("user", OType.STRING).createIndex(INDEX_TYPE.UNIQUE)
      db.declareIntent(new OIntentMassiveInsert())
      db.begin(TXTYPE.NOTX)
      var size = 0
      val doc = new ODocument("user")
      (1 to userCount).foreach { i ⇒
        doc.reset
        doc.setClassName("User")
        doc.field("id", i)
        doc.field("user", "user" + i)
        doc.save()
        size += doc.getSize
      }
      db.declareIntent(null)
    }

    val countUsersSession = OrientDbSession { db ⇒
      db.countClass("User")
    }

    val cleanupUsersAndDb = OrientDbSession { db ⇒
      db.drop()
    }

    val longRunningQuery = OrientDbSession { implicit db ⇒
      db.create()
      createClass("User").createProperty("user", OType.STRING).createIndex(INDEX_TYPE.UNIQUE)
      db.declareIntent(new OIntentMassiveInsert())
      db.begin(TXTYPE.NOTX)
      var size = 0
      val doc = new ODocument("user")
      (1 to 70000).foreach { i ⇒
        doc.reset
        doc.setClassName("User")
        doc.field("id", i)
        doc.field("user", "user" + i)
        doc.save()
        size += doc.getSize
      }
      db.declareIntent(null)
    }
  }
}
