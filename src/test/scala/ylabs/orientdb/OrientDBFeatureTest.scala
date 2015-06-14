package ylabs.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OValidationException
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.scalatest.{BeforeAndAfterAll, WordSpec, GivenWhenThen, ShouldMatchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class OrientDBFeatureTest
    extends WordSpec with ShouldMatchers with GivenWhenThen with BeforeAndAfterAll {

  import OrientDBScala._

  // first need to run the following with console.sh:
  // CREATE DATABASE remote:localhost/test root <root_password> plocal
  val db = new ODatabaseDocumentTx("remote:localhost/test")
  db.open("admin", "admin")

  // CREATE DATABASE remote:localhost/graphtest root root plocal graph
  val graphFactory = new OrientGraphFactory("remote:localhost/graphtest").setupPool(1, 10)
  val graph = graphFactory.getTx

  override def beforeAll(): Unit = {
  }

  override def afterAll() {
    implicit val _db = db
    val classNames = List("User", "Person", "StrictClass", "TestClass")
    classNames.foreach {
      className ⇒
        sqlCommand(s"delete from $className")
        dropClass(className)
    }
  }

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  "As a Document DB" should {

    val userCount = 10000

    s"DB insert $userCount records" in {
      time {
        implicit val _db = db
        createIndex("User", "user", OType.STRING, INDEX_TYPE.UNIQUE)
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

    "Create a schema-full class" in {
      implicit val _db = db
      val schema = getSchema
      val cat = createClass("StrictClass")
      cat.setStrictMode(true)
      createProperty(cat, "name", OType.STRING).setMandatory(true)
      createProperty(cat, "weight", OType.DOUBLE).setMandatory(true)
      schema.save()

      val doc = new ODocument()
      doc.setClassName("StrictClass")

      doc.field("name", "cat1")
      doc.field("weight", 123.0)
      doc.save()

      intercept[OValidationException] {
        doc.field("notAllowedBySchema", 1)
        doc.save()
      }

      db.q[ODocument]("select * from StrictClass").size shouldBe 1

      // the following check fails due to the previously described error in OrientDB
      db.q[ODocument]("select * from StrictClass where rejectedfield=1").size shouldBe 0
    }

    "DB access in futures" in {
      implicit val _db = db

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

  "As a Graph DB" should {

    "DB insert records" in {

      val luca = graph.addVertex()
      luca.setProperty("name", "Luca")

      val marko = graph.addVertex()
      marko.setProperty("name", "Marko")

      val lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows")
      println("Created edge: " + lucaKnowsMarko.getId)

      graph.getVertices.asScala.foreach(vertex ⇒ println(vertex.getProperty("name")))
    }
  }

}
