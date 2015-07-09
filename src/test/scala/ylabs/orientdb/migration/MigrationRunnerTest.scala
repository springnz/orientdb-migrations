package ylabs.orientdb.migration

import org.scalatest.{ ShouldMatchers, WordSpec }
import ylabs.orientdb.test.{ ODBMemoryTest, ODBTestBase }
import ylabs.orientdb.{ ODBScala, ODBSession }

class MigrationRunnerTest extends WordSpec with ShouldMatchers with ODBTestBase with ODBMemoryTest {
  import ODBScala._

  def dbName = "migration-test"
  def classNames = Seq()

  val testClassName = "ylabs.orientdb.migration.TestMigrations"

  "MigrationRunner" should {

    "run a migration" in {
      val executionResult = MigrationRunner.run(Array("db1"), testClassName)
      executionResult.isSuccess shouldBe true

      implicit val db = pool.acquire().get
      val result = ODBSession(implicit db ⇒ selectClass("Person")(identity)).run().get
      result.size shouldBe 1
      result.head.getString("name") shouldBe "bob"
      result.head.getInt("age") shouldBe 123
      db.close()
    }
  }
}
