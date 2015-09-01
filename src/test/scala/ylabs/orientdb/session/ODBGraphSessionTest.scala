package ylabs.orientdb.session

import com.orientechnologies.orient.core.exception.ODatabaseException
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures._
import ylabs.orientdb.pool.{ ODBGraphConnectConfig, ODBGraphConnectionPool }

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{ Success, Try }
import scalaz.syntax.bind._

class ODBGraphSessionTest extends WordSpec {

  implicit val ec = ExecutionContext.global

  implicit val pool = new ODBGraphConnectionPool {
    override def dbConfig: Try[ODBGraphConnectConfig] =
      Success(ODBGraphConnectConfig("memory:test", "admin", "admin", 1, 20))
  }

  "ODBGraphSession" should {
    "flatMap" in new Fixture {
      val session = for {
        vertices1 ← ODBGraphSession { graph ⇒ createVertices(graph, numVertices / 2) }
        vertices2 ← ODBGraphSession { graph ⇒ createVertices(graph, numVertices / 2) }
        vertexCount ← ODBGraphSession { graph ⇒ countVertices(graph) }
        _ ← ODBGraphSession { graph ⇒ addEdges(graph, numEdges, vertices1, vertices2) }
        edgeCount ← ODBGraphSession { graph ⇒ countEdges(graph) }
      } yield {
        (vertexCount, edgeCount)
      }

      val (vertexCount, edgeCount) = session.run().get

      vertexCount shouldBe numVertices
      edgeCount shouldBe numEdges
    }

    "fail when database is closed" in new Fixture {
      val session = for {
        vertices1 ← ODBGraphSession { graph ⇒ createVertices(graph, numVertices / 2) }
        _ ← ODBGraphSession { _.shutdown() }
        vertexCount ← ODBGraphSession { graph ⇒ countVertices(graph) }
      } yield {
        vertexCount
      }

      val result = session.runAsync()(pool, ec)

      whenReady(result.failed, Timeout(3.seconds)) { f ⇒
        f shouldBe a[ODatabaseException]
      }
    }

    "commits changes" in new Fixture {
      val session1 = ODBGraphSession { graph ⇒ deleteAllElements(graph) }
      val session2 = ODBGraphSession { graph ⇒ createVertices(graph, numVertices) }
      val session3 = ODBGraphSession { graph ⇒ countVertices(graph) }

      session1.run()
      session3.run().get shouldBe 0
      session2.run()
      session3.run().get shouldBe numVertices
    }
  }

  trait Fixture {

    val numVertices = 100
    val numEdges = 100

    def createVertices(graph: OrientGraph, count: Int): IndexedSeq[Vertex] = {
      (1 to count) map (_ ⇒ graph.addVertex(null, "lbl", "node"))
    }

    def randomVertex(graph: OrientGraph, vertices: IndexedSeq[Vertex]) = {
      val number = math.random * vertices.size
      graph.getVertex(vertices(number.toInt))
    }

    def addEdges(graph: OrientGraph, count: Int, from: IndexedSeq[Vertex], to: IndexedSeq[Vertex]) = {
      (1 to count) foreach { _ ⇒
        val vertexA = randomVertex(graph, from)
        val vertexB = randomVertex(graph, to)
        graph.addEdge(null, vertexA, vertexB, "edge")
      }
    }

    def countVertices(graph: OrientGraph): Int =
      graph.getVertices.asScala.size

    def countEdges(graph: OrientGraph): Int =
      graph.getEdges.asScala.size

    def deleteAllElements(graph: OrientGraph): Unit = {
      graph.getEdges.asScala.foreach(_.remove())
      graph.getVertices.asScala.foreach(_.remove())
    }
  }
}

