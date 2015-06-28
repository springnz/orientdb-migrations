package ylabs.orientdb

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OResultSet
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass
import com.orientechnologies.orient.core.record.ORecord
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import collection.JavaConversions._
import concurrent.ExecutionContext.Implicits.global
import collection.mutable
import org.apache.tinkerpop.gremlin.orientdb._
import gremlin.scala._
import java.util.{ ArrayList ⇒ JArrayList }

class Gremlin3Test extends WordSpec with ShouldMatchers {

  "vertices" should {
    "be found if they exist" in new Fixture {
      val v1 = sg.addVertex()
      val v2 = sg.addVertex()
      val v3 = sg.addVertex()

      gs.V(v1.id, v3.id).toList should have length 2
      gs.V().toList should have length 3
    }

    "not be found if they don't exist" in new Fixture {
      val list = gs.V("#3:999").toList
      list should have length 0
    }

    "set property after creation" in new Fixture {
      val v = sg.addVertex()
      val key = "testProperty"
      v.setProperty(key, "testValue1")

      v.property[String](key).value shouldBe "testValue1"
      gs.V(v.id).values(key).toList shouldBe List("testValue1")
    }

    "set property during creation" in new Fixture {
      val property1 = "key1" → "value1"
      val property2 = "key2" → "value2"
      val v = sg.addVertex(Map(property1, property2))
      gs.V(v.id).values[String]("key1", "key2").toList shouldBe List("value1", "value2")
    }

    "using labels" in new Fixture {
      val v1 = sg.addVertex("label1")
      val v2 = sg.addVertex("label2")
      val v3 = sg.addVertex()

      val labels = gs.V.label.toSet
      // labels should have size 3
      labels should contain("V")
      labels should contain("label1")
      labels should contain("label2")
    }
  }

  "edges" should {
    "be found if they exist" in new Fixture {
      val v1 = sg.addVertex()
      val v2 = sg.addVertex()
      val e1 = v1.addEdge("label1", v2)
      val e2 = v2.addEdge("label2", v1)

      gs.E(e2.id).toList should have length 1
      gs.E().toList should have length 2
    }

    "not be found if they don't exist" in new Fixture {
      val list = gs.E("#3:999").toList
      list should have length 0
    }

    "set property after creation" in new Fixture {
      val v1 = sg.addVertex()
      val v2 = sg.addVertex()
      val e = v1.addEdge("label1", v2)

      val key = "testProperty"
      e.setProperty(key, "testValue1")

      e.property[String](key).value shouldBe "testValue1"
      gs.E(e.id).values(key).toList shouldBe List("testValue1")
    }

    "set property during creation" in new Fixture {
      val v1 = sg.addVertex()
      val v2 = sg.addVertex()
      val property1 = "key1" → "value1"
      val property2 = "key2" → "value2"
      val e = v1.addEdge("label1", v2, Map(property1, property2))
      gs.E(e.id).values[String]("key1", "key2").toList shouldBe List("value1", "value2")
    }

  }

  "execute arbitrary orient-SQL" in new Fixture {
    (1 to 20) foreach { _ ⇒
      sg.addVertex()
    }

    val results: Seq[_] = graph.executeSql("select from V limit 10") match {
      case lst: JArrayList[_] ⇒ lst.toSeq
      case r: OResultSet[_]   ⇒ r.iterator().toSeq
      case other              ⇒ println(other.getClass()); println(other); ???
    }
    results should have length 10
  }

  trait Fixture {
    // val graph = new OrientGraphFactory("remote:localhost/graphtest", "root", "root").getTx()
    val graph = new OrientGraphFactory(s"memory:test-${math.random}").getTx()
    val gs = GremlinScala(graph)
    val sg = ScalaGraph(graph)
  }

  trait RemoteGraphFixture {
    val graphUri = "remote:localhost/test"
    val graph = new OrientGraphFactory(graphUri, "root", "root").getTx()
    val gs = GremlinScala(graph)
    val sg = ScalaGraph(graph)
  }

}
