package ylabs.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen, ShouldMatchers, WordSpec }

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

class OrientDocumentDBTest
    extends WordSpec with ShouldMatchers with GivenWhenThen with BeforeAndAfterAll
    with OrientDocumentDBScala {

  implicit val db = new ODatabaseDocumentTx("memory:doctest")
  db.create()

  override def beforeAll(): Unit = {
  }

  override def afterAll() {
    val classNames = List("User", "Person", "StrictClass", "TestClass")
    classNames.foreach {
      className ⇒ sqlCommand(s"delete from $className")
    }
  }

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  "Document DB" should {

    val userCount = 10000

    s"DB insert $userCount records" in {
      time {
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
        println("Total Bytes: " + size + ", per record: " + (size / userCount))
        db.declareIntent(null)
        val count = db.countClass("User")

        assert(userCount === count)
      }
    }

    "DB Search" in {
      time {
        val result = db.q[ODocument]("select user from User where user = ?", "user10")
        result.foreach(doc ⇒ println(doc))

        assert(result.head.field("user").toString === "user10")
      }
    }

    "DB select all" in {
      time {
        val result = db.q[ODocument]("select * from User")
        println(s"Retrieved ${result.size} records")
      }
    }

    s"DB delete ${userCount / 2} records" in {
      time {
        db.browseClass("User").iterator.asScala.take(userCount / 2).foreach(_.delete())
        val count = db.countClass("User")
        assert(count === (userCount / 2))
      }
    }

    "Create, insert, update a schema-full class" in {
      val strictClass = createClass("StrictClass")
      strictClass.setStrictMode(true)
      strictClass.createProperty("name", OType.STRING).setMandatory(true).createIndex(INDEX_TYPE.UNIQUE)
      strictClass.createProperty("weight", OType.DOUBLE).setMandatory(true)

      def insert(name: String, weight: Double): ODocument = {
        val doc = new ODocument()
        doc.setClassName("StrictClass")

        doc.field("name", name)
        doc.field("weight", weight)
        doc.save()
      }

      val doc = insert("jones", 123.0)

      intercept[OValidationException] {
        doc.field("notAllowedBySchema", 1)
        doc.save()
      }

      db.count("select count(*) from StrictClass") shouldBe 1
      db.count("select count(*) from StrictClass where rejectedfield=1") shouldBe 0

      intercept[ORecordDuplicatedException] {
        insert("jones", 456.0)
      }

//      val result = sqlCommand("""update StrictClass set name="bob" where name="jones" """).execute().asInstanceOf[java.lang.Integer].toInt
//      result shouldBe 1
//
//      db.count(""" select count(*) from StrictClass where name="jones"  """) shouldBe 0
//      db.count(""" select count(*) from StrictClass where name="bob"  """) shouldBe 1
//
//      sqlCommand(""" delete from StrictClass  """).execute()
//      db.count("select count(*) from StrictClass") shouldBe 0

    }

    "DB access in futures" in {
      val className = "TestClass"

      val f = dbFuture {
        val testClass = createClass(className)
        val doc = new ODocument()
        doc.setClassName(className)
        doc.field("id", 1)
        doc.save()
      }

      Await.result(f, 10.seconds)

      val result = db.q[ODocument](s"select * from $className where id = 1")
      result.size shouldBe 1
    }
  }

  "Works with JSON" should {
    "Insert JSON" in {
      val doc = new ODocument("Person")
      val json =
        """
          |{
          |  "gender": {"name": "Male"},
          |  "firstName": "Robert",
          |  "lastName": "Smith",
          |  "account": {"checking": 10, "savings": 1234}
          |}
        """.stripMargin

      doc.fromJSON(json)
      doc.setClassName("Person")
      doc.save()
      assert(db.countClass("Person") === 1)
    }

    "Search JSON" in {
      val result = db.q[ODocument]("select account[savings] from Person")
      result.foreach(println)

      assert(Int.unbox(result.head.field("account")) === 1234)
    }
  }
}
