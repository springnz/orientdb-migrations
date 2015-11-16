package springnz.orientdb.migration

import org.scalatest.{Matchers, WordSpec}
import springnz.orientdb.session.ODBSession
import springnz.orientdb.test.{ODBMemoryTest, ODBTestBase}

class MigrationTest extends WordSpec with Matchers with ODBTestBase with ODBMemoryTest {
  def dbName = "migrationtest"
  def classNames = Seq()

  val personClass = "Person"

  "creates a class" in {
    val migration = Migration(
      0,
      ODBSession { implicit db ⇒
        createClass(personClass)
      }
    )
    Migrator.runMigration(Seq(migration)) shouldBe 'success

    val clazz = ODBSession { implicit db ⇒
      findClass(personClass)
    }.run().get
    clazz.getName() shouldBe personClass
  }
}
