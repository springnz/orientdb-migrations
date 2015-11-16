package springnz.orientdb.migration

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OType
import java.util.UUID
import org.scalatest.BeforeAndAfterEach
import org.scalatest.{Matchers, WordSpec}
import springnz.orientdb.session.ODBSession
import springnz.orientdb.test.{ODBMemoryTest, ODBTestBase}
import collection.JavaConversions._

class MigrationTest extends WordSpec with Matchers with ODBTestBase with ODBMemoryTest with BeforeAndAfterEach {
  def dbName = "migrationtest"
  val classNames = List(MigrationLog.className)
  def newClass = "Person-" + UUID.randomUUID.toString

  override def beforeEach(): Unit = {
    deleteClassRecords()
  }

  "creates a class" in {
    val personClass = newClass
    runMigration { implicit db ⇒ createClass(personClass) }

    val clazz = execute { implicit db ⇒ findClass(personClass) }
    clazz.getName() shouldBe personClass
  }

  "creates a class with property" in {
    val personClass = newClass
    val property = "propName"
    runMigration { implicit db ⇒
      val clazz = createClass(personClass)
      clazz.createProperty(property, OType.STRING)
    }

    val clazz = execute { implicit db ⇒ findClass(personClass) }
    Option(clazz.getProperty(property)) shouldBe 'defined
  }

  "creates a vertex class" in {
    val personClass = newClass
    runMigration { implicit db ⇒ createVertexClass(personClass) }

    val clazz = execute { implicit db ⇒ findClass(personClass) }
    clazz.getName() shouldBe personClass
    clazz.getSuperClassesNames().toList shouldBe List("V")
  }

  "creates an edge class" in {
    val personClass = newClass
    runMigration { implicit db ⇒ createEdgeClass(personClass) }

    val clazz = execute { implicit db ⇒ findClass(personClass) }
    clazz.getName() shouldBe personClass
    clazz.getSuperClassesNames().toList shouldBe List("E")
  }

  def runMigration[A](block: ODatabaseDocumentTx ⇒ A): Unit = {
    val migration = Migration(
      0,
      ODBSession { db ⇒ block(db) }
    )
    Migrator.runMigration(Seq(migration)) shouldBe 'success
  }

  def execute[A](block: ODatabaseDocumentTx => A): A =
    ODBSession { db ⇒ block(db) }.run().get
}
