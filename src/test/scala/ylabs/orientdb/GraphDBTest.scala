package ylabs.orientdb

import com.tinkerpop.blueprints.impls.orient._
import org.scalatest.{ ShouldMatchers, WordSpec }
import ylabs.orientdb.test.ODBRemoteTestTag

import scala.collection.JavaConverters._
import scala.collection.mutable

class GraphDBTest extends WordSpec with ShouldMatchers {

  // first need to run the following with console.sh:
  // CREATE DATABASE remote:localhost/graphtest root root plocal graph
  // val graphFactory = new OrientGraphFactory("remote:localhost/graphtest")
  // val graphFactory = new OrientGraphFactory("plocal:target/databases/test" + math.random)
  val graphFactory = new OrientGraphFactory("memory:test")
  val graph = graphFactory.setupPool(1, 10).getNoTx

  object Labels extends Enumeration {
    val Listing, User, Session, ViewListing, ViewNumber = Value
  }

  import Labels._
  type Id = String

  val ids = mutable.Map.empty[Labels.Value, Seq[Id]].withDefaultValue(Seq.empty)

  def createVertices(count: Int, label: Labels.Value): Unit = {
    (1 to count) foreach { i ⇒
      val v = graph.addVertex(null, "lbl", label.toString)
      ids.update(label, ids(label) :+ v.getId.toString)
      // if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
    }
  }

  def randomVertex(label: Labels.Value) = {
    val list = ids(label)
    val number = math.random * list.size
    graph.getVertex(list(number.toInt))
  }

  def addListingViews(count: Int) =
    (1 to count) foreach { _ ⇒
      val user = randomVertex(User)
      val listing = randomVertex(Listing)
      graph.addEdge(null, user, listing, ViewListing.toString)
      //TODO: add a random property to this, e.g. the time
      // user.addEdge(ViewListing.toString, listing, Map(s"random property ${math.random}" → math.random))
    }

  "tinkerpop api" should {

    "create scenario graph" taggedAs ODBRemoteTestTag in {
      println("starting to delete the existing elements")
      graph.getEdges.asScala.foreach(_.remove())
      graph.getVertices.asScala.foreach(_.remove())
      println("deleted all elements")
      // time {
      //   (1 to 1000000) foreach { i ⇒
      //     val v = graph.addVertex(null, "name", s"name $i")
      //     // println(v.getBaseClassName)

      //     if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
      //   }
      //   // graph.declareIntent(null)
      // }

      time {
        createVertices(100, Listing)
        createVertices(100, User)
        addListingViews(200)
      }

      println("vertex count: " + graph.getVertices.asScala.size)
      println("edge count: " + graph.getEdges.asScala.size)
    }

    "insert some DB records" ignore {
      time {
        val luca = graph.addVertex(null, "name", "Luca")
        val marko = graph.addVertex(null, "name", "Marko")
        val lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows")

        graph.getVertices.asScala.foreach(vertex ⇒ println(vertex.getProperty("name")))
      }
    }
  }

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }
}
