package ylabs.orientdb

import org.scalatest._
import ylabs.orientdb.session.ODBGraphSession
import ylabs.orientdb.test.ODBGraphMemoryTest

import scala.collection.JavaConverters._

class OrientGraphDBTest extends WordSpec with ShouldMatchers with ODBGraphMemoryTest {

  "Graph DB" should {

    "DB insert records" in {
      ODBGraphSession {
        graph ⇒
          val luca = graph.addVertex()
          luca.setProperty("name", "Luca")

          val marko = graph.addVertex()
          marko.setProperty("name", "Marko")

          val lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows")
          println("Created edge: " + lucaKnowsMarko.getId)

          graph.getVertices.asScala.foreach(vertex ⇒ println(vertex.getProperty("name")))
      }.run()
    }
  }
}
