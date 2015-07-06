package ylabs.orientdb.migration

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec }
import ylabs.orientdb.ODBScala._
import ylabs.orientdb.ODBSession
import ylabs.orientdb.test.ODBTestBase
import ylabs.util.DateTimeUtil
import ylabs.util.Pimpers._

trait MigratorTest extends WordSpec with ShouldMatchers with BeforeAndAfterEach with BeforeAndAfterAll with ODBTestBase {

  val dbName = "migrator-test"

  val classNames = List(MigrationLog.className)

  override def beforeEach(): Unit = {
    deleteClassRecords()
  }

  "Migrator" should {

    "handle one successful migration" in new Fixture {
      val migration = Migration(version, successMigration)

      Migrator.runMigration(Seq(migration))

      val migrationLogs = Migrator.fetchMigrationLogs().run().get
      migrationLogs.size shouldBe 1
      val migrationLog = migrationLogs.head
      migrationLog.version shouldBe version
      migrationLog.timestamp should (be >= utcNow.minusSeconds(2) and be <= utcNow.plusSeconds(2))
    }

    "handle one failed migration" in new Fixture {
      val migration = Migration(version, failureMigration)

      Migrator.runMigration(Seq(migration))

      val migrationLogs = Migrator.fetchMigrationLogs().run().get
      migrationLogs.size shouldBe 0
      failedMigrationCounter shouldBe 1
    }

    "handle multiple successful migrations" in new Fixture {
      val versions = Seq(2, 4, 5)
      val migrations = versions.map(Migration(_, successMigration))

      Migrator.runMigration(migrations)

      val migrationLogs = Migrator.fetchMigrationLogs().run().get
      migrationLogs.map(_.version) shouldBe versions
      successfulMigrationCounter shouldBe 3
    }

    "skip existing migrations" in new Fixture {
      val migrations = (1 to 3).map(Migration(_, successMigration))

      Migrator.runMigration(migrations)
      Migrator.runMigration(migrations)

      val migrationLogs = Migrator.fetchMigrationLogs().run().get
      migrationLogs.size shouldBe 3
      successfulMigrationCounter shouldBe 3
    }

    "stop migration sequence when a migration fails" in new Fixture {
      val migrations = Seq(
        Migration(1, successMigration),
        Migration(2, successMigration),
        Migration(3, failureMigration),
        Migration(4, successMigration))

      Migrator.runMigration(migrations)

      val migrationLogs = Migrator.fetchMigrationLogs().run().get
      migrationLogs.map(_.version) shouldBe Seq(1, 2)
      successfulMigrationCounter shouldBe 2
      failedMigrationCounter shouldBe 1
    }

    "execute a schema migration" in new Fixture {
      val migration1: ODBSession[Unit] = ODBSession { implicit db ⇒
        val oClass = createClass("Person")
        oClass.createProperty("name", OType.STRING)
      }
      val migration2: ODBSession[Unit] = ODBSession { implicit db ⇒
        val oClass = findClass("Person")
        oClass.createProperty("age", OType.INTEGER)
      }
      val migration3: ODBSession[Unit] = ODBSession { implicit db ⇒
        val doc = new ODocument("Person")
        doc.field("name", "bob")
        doc.field("age", 123)
        doc.save()
      }

      val migrations = Seq(Migration(1, migration1), Migration(2, migration2), Migration(3, migration3))

      Migrator.runMigration(migrations)

      val result = ODBSession(implicit db ⇒ selectClass("Person")(identity)).run().get
      result.size shouldBe 1
      result.head.getString("name") shouldBe "bob"
      result.head.getInt("age") shouldBe 123

      val migrationLogs = Migrator.fetchMigrationLogs().run().get
      migrationLogs.map(_.version) shouldBe Seq(1, 2, 3)
    }
  }

  trait Fixture {
    val version = 1
    val utcNow = DateTimeUtil.utcDateTime

    var successfulMigrationCounter = 0
    var failedMigrationCounter = 0

    val successMigration: ODBSession[Unit] = ODBSession {
      implicit db ⇒
        successfulMigrationCounter += 1
    }

    val failureMigration: ODBSession[Unit] = ODBSession {
      implicit db ⇒
        failedMigrationCounter += 1
        throw new Exception("fail")
    }
  }

}
