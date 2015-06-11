
import OrientDBScala._
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, GivenWhenThen}

import scala.collection.JavaConverters._

class OrientDBFeatureTest extends FeatureSpec with GivenWhenThen with BeforeAndAfterAll {

  // create a document db on disk
  val db: ODatabaseDocumentTx = new ODatabaseDocumentTx("local:/tmp/testdb")
  if (!db.exists) db.create() else db.open("admin", "admin")

  override def beforeAll(): Unit = {
  }

  override def afterAll() {
    db.getMetadata.getSchema.dropClass("User")
    db.getMetadata.getSchema.dropClass("Person")
    db.close()
  }

  //run tests
  feature("As a Document DB") {

    val userCount = 1000

    scenario(s"DB insert $userCount records") {
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
        size += doc.getSize
        doc.save
      }
      println("Total Bytes: " + size + ", per record: " + (size / userCount))
      db.declareIntent(null)
      val count = db.countClass("User")

      assert(userCount === count)
    }

    scenario("DB Search") {
      val result = db.q[ODocument]("select user from User where user = 'user10'")
      result.foreach(doc ⇒ println(doc))

      assert(result.head.field("user").toString === "user10")
    }

    scenario(s"DB delete ${userCount / 2} records") {
      db.browseClass("User").iterator.asScala.take(userCount / 2).foreach(_.delete())
      val count = db.countClass("User")
      assert(count === (userCount / 2))
    }
  }

  feature("Works with JSON") {
    scenario("Insert JSON") {
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

    scenario("Search JSON") {
      val result = db.q[ODocument]("select account[savings] from Person")
      result.foreach(doc ⇒ println(doc))

      assert(Int.unbox(result.head.field("account")) === 1234)
    }
  }

  //
  //  feature("As a Graph DB") {
  //
  //    scenario("DB insert records") {
  //
  //      def traverse(inNode: OGraphVertex)(op: OGraphVertex ⇒ Unit) {
  //        op(inNode)
  //        for (node ← inNode.browseOutEdgesVertexes.asScala) yield {
  //          traverse(node)(op)
  //        }
  //      }
  //
  //      val graph: ODatabaseGraphTx = new ODatabaseGraphTx("memory:graph")
  //      graph.create()
  //
  //      val root: OGraphVertex = graph.createVertex.set("id", "root")
  //
  //      val a: OGraphVertex = graph.createVertex.set("id", "_a")
  //      val b: OGraphVertex = graph.createVertex.set("id", "_b")
  //      val c: OGraphVertex = graph.createVertex.set("id", "_c")
  //
  //      val a1: OGraphVertex = graph.createVertex.set("id", "__a1")
  //      val a2: OGraphVertex = graph.createVertex.set("id", "__a2")
  //      val b1: OGraphVertex = graph.createVertex.set("id", "__b1")
  //
  //      root.link(a)
  //      root.link(b)
  //      root.link(c)
  //      a.link(a1)
  //      a.link(a2)
  //
  //      b.link(b1).save
  //
  //      traverse(root)((n: OGraphVertex) ⇒ {
  //        n.save
  //        println(n.get("id"))
  //      })
  //
  //      val result = graph.q("select from OGraphVertex")
  //
  //      assert(result.size === 7)
  //
  //    }
  //  }

}
