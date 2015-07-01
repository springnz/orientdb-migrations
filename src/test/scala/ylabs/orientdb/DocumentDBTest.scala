package ylabs.orientdb

import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import org.scalatest._
import ylabs.orientdb.test.ODBTestBase
import ylabs.util.Pimpers._

import scala.collection.JavaConverters._
import scala.util.Try

trait DocumentDBTest
    extends WordSpec with ShouldMatchers with GivenWhenThen with BeforeAndAfterAll with BeforeAndAfterEach
    with OrientDocumentDBScala with ODBTestBase {

  val dbName = "document-db-test"

  val classNames = List("User", "Person", "StrictClass", "TestClass")

  def dropClasses(): Unit = OrientDbSession { implicit db ⇒
    classNames.foreach(className ⇒ Try { dropClass(className) })
  }.run().withErrorLog("failed to drop classes")

  def deleteClassRecords(): Unit = OrientDbSession { implicit db ⇒
    classNames.foreach { className ⇒
      Try { sqlCommand(s"delete from $className").execute().asInstanceOf[java.lang.Integer] }
    }
  }.run().withErrorLog("failed to delete class records")

  override def beforeAll(): Unit = {
    dropClasses()
  }

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  "A strict class" should {

    "work correctly" taggedAs dbTestTag in new StrictClassFixture {
      implicit val db = pool.acquire().get

      Given("a strict class definition")
      val strictClass = createClass(className)
      strictClass.setStrictMode(true)
      strictClass.createProperty(nameField, OType.STRING).setMandatory(true).createIndex(INDEX_TYPE.UNIQUE)
      strictClass.createProperty(ageField, OType.INTEGER).setMandatory(true)

      When("inserting duplicates")
      insert("jones", 30)

      Then("it should throw an exception")
      intercept[ORecordDuplicatedException] {
        insert("jones", 40)
      }

      When("updating a record")
      val result = sqlCommand("""update StrictClass set name="bob" where name="jones" """)
        .execute().asInstanceOf[java.lang.Integer].toInt

      Then("it should return the number of updated rows")
      result shouldBe 1

      And("the record should be updated")
      db.count(""" select count(*) from StrictClass where name="jones"  """) shouldBe 0
      db.count(""" select count(*) from StrictClass where name="bob"  """) shouldBe 1

      And("the selectClass method should pick up the updated record")
      selectClass(className)(identity).head.field(nameField).asInstanceOf[String] shouldBe "bob"

      When("all records are deleted")
      sqlCommand(""" delete from StrictClass  """).execute()

      Then("the count should be zero")
      db.count("select count(*) from StrictClass") shouldBe 0

      Given("a record with a field not allowed by the schema")
      val doc = new ODocument(className)
      doc.field(nameField, "jimmy")
      doc.field(ageField, 123)
      doc.field("notAllowedBySchema", 1)

      When("saving")
      Then("it should throw an exception")
      intercept[OValidationException] {
        doc.save()
      }

      db.close()
    }
  }

  trait StrictClassFixture {
    val className = "StrictClass"
    val nameField = "name"
    val ageField = "age"

    def insert(name: String, age: Int): ODocument = {
      val doc = new ODocument(className)
      doc.field(nameField, name)
      doc.field(ageField, age)
      doc.save()
    }
  }

  "Document DB" should {

    val userCount = 10000
    val className = "User"
    val fieldName = "name"

    s"DB insert $userCount records" taggedAs dbTestTag in {
      implicit val db = pool.acquire().get
      time {
        createClass(className).createProperty(fieldName, OType.STRING).createIndex(INDEX_TYPE.UNIQUE)
        db.declareIntent(new OIntentMassiveInsert())
        db.begin(TXTYPE.NOTX)
        var size = 0
        val doc = new ODocument()
        (1 to userCount).foreach { i ⇒
          doc.reset
          doc.setClassName(className)
          doc.field("id", i)
          doc.field(fieldName, "user" + i)
          doc.save()
          size += doc.getSize
        }
        println("Total Bytes: " + size + ", per record: " + (size / userCount))
        db.declareIntent(null)

        val count = db.countClass(className)

        assert(userCount === count)
      }
      db.close()
    }

    "DB Search" taggedAs dbTestTag in {
      implicit val db = pool.acquire().get
      time {
        val result = db.q[ODocument](s"select $fieldName from $className where $fieldName = ?", "user10")
        assert(result.head.field(fieldName).toString === "user10")
      }
      db.close()
    }

    "DB select all" taggedAs dbTestTag in {
      implicit val db = pool.acquire().get
      time {
        val result = db.q[ODocument]("select * from User")
        result.size shouldBe userCount
      }
      db.close()
    }

    s"DB delete ${userCount / 2} records" taggedAs dbTestTag in {
      implicit val db = pool.acquire().get
      time {
        db.browseClass("User").iterator.asScala.take(userCount / 2).foreach(_.delete())
        val count = db.countClass("User")
        assert(count === (userCount / 2))
      }
      db.close()
    }
  }

  "Works with JSON" should {
    "Insert JSON" taggedAs dbTestTag in new JsonFixture {
      implicit val db = pool.acquire().get
      val doc = new ODocument("Person")
      doc.fromJSON(json)
      doc.setClassName("Person")
      doc.save()
      assert(db.countClass("Person") === 1)
      db.close()
    }

    "Search JSON" taggedAs dbTestTag in new JsonFixture {
      implicit val db = pool.acquire().get
      val doc = new ODocument("Person")
      doc.fromJSON(json)
      doc.setClassName("Person")
      doc.save()

      val result = db.q[ODocument]("select account[savings] from Person")
      assert(Int.unbox(result.head.field("account")) === 1234)
      db.close()
    }
  }

  trait JsonFixture {
    val json =
      """
        |{
        |  "gender": {"name": "Male"},
        |  "firstName": "Robert",
        |  "lastName": "Smith",
        |  "account": {"checking": 10, "savings": 1234}
        |}
      """.stripMargin
  }
}
