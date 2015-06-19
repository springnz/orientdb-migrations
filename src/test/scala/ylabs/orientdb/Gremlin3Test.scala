package ylabs.orientdb

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import collection.JavaConversions._
import concurrent.ExecutionContext.Implicits.global
import collection.mutable
import org.apache.tinkerpop.gremlin.orientdb.structure._
import gremlin.scala._

class Gremlin3Test extends WordSpec with ShouldMatchers {
  // import OrientDBScala._

  // first need to run the following with console.sh:
  // CREATE DATABASE remote:localhost/graphtest root root plocal graph
  val graph = new OrientGraphFactory().open("remote:localhost/graphtest", "root", "root")
  val gs = GremlinScala(graph)

  "tinkerpop3 api" should {
    "lookup vertices by id" in {
      val list = gs.V("#9:202", "#9:210").toList
      list should have length 2
    }

    "set property on edge" in {
      val v = gs.V.head
      val key = "testProperty"
      v.property(key, "testValue1")

      v.property[String](key).value shouldBe "testValue1"
      gs.V(v.id).values(key).toList shouldBe List("testValue1")
    }
  }


  // val graphFactory = new OrientGraphFactory().open("remote:localhost/graphtest")
  // val graphFactory = new OrientGraphFactory("plocal:target/databases/test" + math.random)
  // val graphFactory = new OrientGraphFactory("memory:test")
  // val graph = graphFactory.setupPool(1, 10)
    // .getTx
  // .getNoTx

  // object Labels extends Enumeration {
  //   val Listing, User, Session, ViewListing, ViewNumber = Value
  // }
  // import Labels._
  // type Id = String

  // val ids = mutable.Map.empty[Labels.Value, Seq[Id]].withDefaultValue(Seq.empty)

  // def createVertices(count: Int, label: Labels.Value): Unit = {
  //   (1 to count) foreach { i ⇒
  //     val v = graph.addVertex(null, "lbl", label.toString)
  //     ids.update(label, ids(label) :+ v.getId.toString)
  //     // if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
  //   }
  // }

  // def randomVertex(label: Labels.Value) = {
  //   val list = ids(label)
  //   val number = math.random * list.size
  //   graph.getVertex(list(number.toInt))
  // }

  // def addListingViews(count: Int) =
  //   (1 to count) foreach { _ ⇒
  //     val user = randomVertex(User)
  //     val listing = randomVertex(Listing)
  //     graph.addEdge(null, user, listing, ViewListing.toString)
  //     //TODO: add a random property to this, e.g. the time
  //     // user.addEdge(ViewListing.toString, listing, Map(s"random property ${math.random}" → math.random))
  //   }

  // "tinkerpop api" should {
  //   "create scenario graph" ignore {
  //     println("starting to delete the existing elements")
  //     graph.getEdges.asScala.foreach(_.remove())
  //     graph.getVertices.asScala.foreach(_.remove())
  //     println("deleted all elements")
  //     // time {
  //     //   (1 to 1000000) foreach { i ⇒
  //     //     val v = graph.addVertex(null, "name", s"name $i")
  //     //     // println(v.getBaseClassName)

  //     //     if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
  //     //   }
  //     //   // graph.declareIntent(null)
  //     // }

  //     time {
  //       createVertices(100, Listing)
  //       createVertices(100, User)
  //       addListingViews(200)
  //     }

  //     println("vertex count: " + graph.getVertices.asScala.size)
  //     println("edge count: " + graph.getEdges.asScala.size)


  //     // println(graph.getVertices)
  //     // val a: OrientVertex = ???
  //     // println(graph.getVertices.asScala)
  //     // println(graph.getEdges.asScala)
  //     // graph.getVertices.asScala.foreach(vertex ⇒ println(vertex.getProperty("name")))
  //   }

  //   "insert some DB records" ignore {
  //     time {
  //       val luca = graph.addVertex(null, "name", "Luca")
  //       val marko = graph.addVertex(null, "name", "Marko")
  //       val lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows")

  //       graph.getVertices.asScala.foreach(vertex ⇒ println(vertex.getProperty("name")))
  //     }
  //   }
  // }

  // def time[R](block: ⇒ R): R = {
  //   val t0 = System.nanoTime()
  //   val result = block // call-by-name
  //   val t1 = System.nanoTime()
  //   println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
  //   result
  // }
}
