package ylabs.orientdb.session

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import gremlin.scala.ScalaGraph

final case class ODBGremlinSession[+A](override val block: ScalaGraph[OrientGraph] ⇒ A)
  extends AbstractODBSession[A, ScalaGraph[OrientGraph]](block) {

  def run(graph: ScalaGraph[OrientGraph]): A = {
    val result = block(graph)
    close(graph)
    result
  }

  def close(graph: ScalaGraph[OrientGraph]): Unit =
    graph.graph.close()
}

object ODBGremlinSession extends ODBSessionInstances[ScalaGraph[OrientGraph], ODBGremlinSession]
