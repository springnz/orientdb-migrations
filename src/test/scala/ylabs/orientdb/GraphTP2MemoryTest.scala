package ylabs.orientdb

import com.tinkerpop.blueprints.impls.orient._
import org.scalatest.{ ShouldMatchers, WordSpec }
import ylabs.orientdb.session.ODBTP2Session
import ylabs.orientdb.test.{ ODBMemoryTestTag, ODBTP2MemoryTest }
import ylabs.util.Logging

import scala.collection.JavaConverters._

class GraphTP2MemoryTest extends WordSpec with ShouldMatchers with ODBTP2MemoryTest with Logging {

  // first need to run the following with console.sh:
  // CREATE DATABASE remote:localhost/graphtest root root plocal graph
  // val graphFactory = new OrientGraphFactory("remote:localhost/graphtest")
  // val graphFactory = new OrientGraphFactory("plocal:target/databases/test" + math.random)

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    log.info("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  "TinkerPop2 API" should {

    "create scenario graph" taggedAs ODBMemoryTestTag in new Fixture1 {
      import Labels._

      ODBTP2Session { implicit graph ⇒
        deleteAllElements()
        createVertices(100, Listing)
        createVertices(100, User)
        addListingViews(200)
      }.run()
      ODBTP2Session(_.getVertices.asScala.size).run().get shouldBe 200
      ODBTP2Session(_.getEdges.asScala.size).run().get shouldBe 200
    }

    "insert some DB records" taggedAs ODBMemoryTestTag in {
      time {
        val names = ODBTP2Session { implicit graph ⇒
          val luca = graph.addVertex(null, "name", "Luca")
          val marko = graph.addVertex(null, "name", "Marko")
          val lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows")
          graph.getVertices.asScala
            .flatMap(vertex ⇒ Option(vertex.getProperty[String]("name")))
            .toSet
        }.run().get
        names shouldBe Set("Luca", "Marko")
      }
    }
  }

  trait Fixture1 {

    object Labels extends Enumeration {
      val Listing, User, Session, ViewListing, ViewNumber = Value
    }
    import Labels._

    type Id = String

    def createVertices(count: Int, label: Labels.Value)(implicit graph: OrientGraph): Unit = {
      log.info(s"Creating $count vertices")
      (1 to count) foreach { i ⇒
        val v = graph.addVertex(null, "lbl", label.toString)
        // if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
      }
    }

    def randomVertex(label: Labels.Value)(implicit graph: OrientGraph) = {
      val vertices = graph.getVertices("lbl", label.toString).asScala.toIndexedSeq
      val number = math.random * vertices.size
      val selectedId = vertices(number.toInt)
      graph.getVertex(selectedId)
    }

    def addListingViews(count: Int)(implicit graph: OrientGraph) = {
      log.info(s"Creating $count edges")
      (1 to count) foreach { _ ⇒
        val user = randomVertex(User)
        val listing = randomVertex(Listing)
        graph.addEdge(null, user, listing, ViewListing.toString)
      }
    }

    def deleteAllElements()(implicit graph: OrientGraph): Unit = {
      log.info("Deleting all graph elements... ")
      graph.getEdges.asScala.foreach(_.remove())
      graph.getVertices.asScala.foreach(_.remove())
      log.info("done")
    }

  }
}
