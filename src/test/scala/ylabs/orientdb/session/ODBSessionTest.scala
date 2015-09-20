package ylabs.orientdb.session

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
import ylabs.orientdb.ODBScala
import ylabs.orientdb.pool.{ ODBConnectConfig, ODBConnectionPool }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{ Success, Try }
import scalaz.syntax.bind._

class ODBSessionTest extends WordSpec with BeforeAndAfterEach with BeforeAndAfterAll with ODBScala {

  val random = scala.util.Random
  implicit val ec = ExecutionContext.global

  "ODBSession" should {

    "flatmap" in new Fixture {
      val session = for {
        x ← createDbAndUsers
        y ← countUsersSession
        _ ← cleanupUsersAndDb
      } yield y

      val result = session.runAsync()

      whenReady(result, Timeout(3.seconds)) { f ⇒
        f shouldBe userCount
      }
    }

    "fail when database is closed" in new Fixture {
      val killSession = ODBSession {
        _.close()
      }

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
  }

  trait Fixture {
    val userCount = 10

    implicit val pool = new ODBConnectionPool {
      def dbConfig: Try[ODBConnectConfig] = Success(ODBConnectConfig("memory:doctest-session", "admin", "admin"))
    }

    val createDbAndUsers = ODBSession { implicit db ⇒
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

    val countUsersSession = ODBSession { db ⇒
      db.countClass("User")
    }

    val cleanupUsersAndDb = ODBSession { db ⇒
      db.drop()
    }

    val longRunningQuery = ODBSession { implicit db ⇒
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
