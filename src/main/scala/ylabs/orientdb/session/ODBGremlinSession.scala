package ylabs.orientdb.session

import gremlin.scala.ScalaGraph

final case class ODBGremlinSession[+A](override val block: ScalaGraph ⇒ A)
  extends AbstractODBSession[A, ScalaGraph](block) {

  def run(graph: ScalaGraph): A = {
    val result = block(graph)
    close(graph)
    result
  }

  def close(graph: ScalaGraph): Unit =
    graph.graph.close()
}

object ODBGremlinSession extends ODBSessionInstances[ScalaGraph, ODBGremlinSession]
