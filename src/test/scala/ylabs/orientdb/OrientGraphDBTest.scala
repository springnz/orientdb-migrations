package ylabs.orientdb

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.scalatest._

import scala.collection.JavaConverters._

class OrientGraphDBTest extends WordSpec with ShouldMatchers with GivenWhenThen with BeforeAndAfterAll {

  val graphFactory = new OrientGraphFactory("memory:graphtest").setupPool(1, 10)
  val graph = graphFactory.getTx

  "Graph DB" should {

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
