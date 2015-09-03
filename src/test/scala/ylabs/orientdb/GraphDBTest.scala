package ylabs.orientdb

import com.tinkerpop.blueprints.impls.orient._
import org.scalatest.{ ShouldMatchers, WordSpec }
import ylabs.orientdb.session.ODBGraphSession
import ylabs.orientdb.test.{ ODBGraphMemoryTest, ODBMemoryTestTag }
import ylabs.util.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable

class GraphDBTest extends WordSpec with ShouldMatchers with ODBGraphMemoryTest with Logging {

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

  "TinkerPop API" should {

    "create scenario graph" taggedAs ODBMemoryTestTag in new Fixture1 {
      import Labels._

      ODBGraphSession { implicit graph ⇒
        deleteAllElements()
        val ids1 = createVertices(3, Listing)
        val ids2 = createVertices(3, User)
        addListingViews(3, ids1 ++ ids2)
      }.run()
      Thread.sleep(1000)
      ODBGraphSession(_.getVertices.asScala.size).run().get shouldBe 20
      ODBGraphSession(_.getEdges.asScala.size).run().get shouldBe 3
    }

    "insert some DB records" taggedAs ODBMemoryTestTag in {
      time {
        val names = ODBGraphSession { implicit graph ⇒
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

    def createVertices(count: Int, label: Labels.Value)(implicit graph: OrientGraph): mutable.Map[Labels.Value, Seq[Id]] = {
      val ids = mutable.Map.empty[Labels.Value, Seq[Id]].withDefaultValue(Seq.empty)
      log.info(s"Creating $count vertices")
      (1 to count) foreach { i ⇒
        val v = graph.addVertex(null, "lbl", label.toString)
        ids.update(label, ids(label) :+ v.getId.toString)
        // if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
      }
      println(s"created vertices with label $label: $ids")
      ids
    }

    def randomVertex(label: Labels.Value, ids: mutable.Map[Labels.Value, Seq[Id]])(implicit graph: OrientGraph) = {
      val list = ids(label)
      val number = math.random * list.size
      val vertices = graph.getVertices().asScala
      println(s"vertices = $vertices")
      val selectedId = list(number.toInt)
      println(s"selected id = $selectedId")
      graph.getVertex(selectedId)
    }

    def addListingViews(count: Int, ids: mutable.Map[Labels.Value, Seq[Id]])(implicit graph: OrientGraph) = {
      log.info(s"Creating $count edges")
      (1 to count) foreach { _ ⇒
        println("ADDING AN EDGE")
        val user = randomVertex(User, ids)
        val listing = randomVertex(Listing, ids)
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
