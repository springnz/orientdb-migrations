package ylabs.orientdb

import com.tinkerpop.blueprints.impls.orient._
import org.scalatest.{ ShouldMatchers, WordSpec }
import ylabs.orientdb.test.ODBMemoryTestTag

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
    println(s"creating $count vertices")
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

  def addListingViews(count: Int) = {
    println(s"creating $count edges")
    (1 to count) foreach { _ ⇒
      val user = randomVertex(User)
      val listing = randomVertex(Listing)
      graph.addEdge(null, user, listing, ViewListing.toString)
      //TODO: add a random property to this, e.g. the time
      // user.addEdge(ViewListing.toString, listing, Map(s"random property ${math.random}" → math.random))
    }
  }

  def deleteAllElements(graph: OrientGraphNoTx): Unit = {
    println("starting to delete the existing elements")
    graph.getEdges.asScala.foreach(_.remove())
    graph.getVertices.asScala.foreach(_.remove())
    println("deleted all elements")
  }

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  "tinkerpop api" should {

    "create scenario graph" taggedAs ODBMemoryTestTag in {
      deleteAllElements(graph)
      time {
        createVertices(100, Listing)
        createVertices(100, User)
        addListingViews(200)
      }
      graph.getVertices.asScala.size shouldBe 200
      graph.getEdges.asScala.size shouldBe 200
    }

    "insert some DB records" taggedAs ODBMemoryTestTag in {
      time {
        val luca = graph.addVertex(null, "name", "Luca")
        val marko = graph.addVertex(null, "name", "Marko")
        val lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows")

        val names = graph.getVertices.asScala
          .flatMap(vertex ⇒ Option(vertex.getProperty[String]("name")))
          .toSet
        names shouldBe Set("Luca", "Marko")
      }
    }
  }
}
