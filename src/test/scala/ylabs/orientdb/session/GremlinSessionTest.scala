package ylabs.orientdb.session

import java.util.{ ArrayList ⇒ JArrayList }

import com.orientechnologies.orient.core.sql.query.OResultSet
import org.apache.tinkerpop.gremlin.orientdb._
import org.scalatest.{ ShouldMatchers, WordSpec }
import ylabs.orientdb.pool.{ ODBConnectConfig, ODBGremlinConnectionPool }

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.util.{ Success, Try }

class GremlinSessionTest extends WordSpec with ShouldMatchers {

  implicit val ec = ExecutionContext.global

  "vertices" should {
    "be found if they exist" in new Fixture {
      val result = ODBGremlinSession { sg ⇒
        val v1 = sg.addVertex()
        val v2 = sg.addVertex()
        val v3 = sg.addVertex()
        sg.V(v1.id, v3.id).toList
      }.run()(pool).get

      result should have length 2
    }
  }

  "execute arbitrary OrientSQL" in new Fixture {
    ODBGremlinSession { sg ⇒
      (1 to 20) foreach { _ ⇒
        sg.addVertex()
      }
    }.run()(pool)

    val results: Seq[_] = ODBGremlinSession { sg ⇒
      sg.graph.asInstanceOf[OrientGraph].executeSql("select from V limit 10") match {
        case lst: JArrayList[_] ⇒ lst.toSeq
        case r: OResultSet[_]   ⇒ r.iterator().toSeq
        case other              ⇒ println(other.getClass); println(other); ???
      }
    }.run()(pool).get
    results should have length 10
  }

  trait Fixture {
    implicit val pool = new ODBGremlinConnectionPool {
      override def dbConfig: Try[ODBConnectConfig] =
        Success(ODBConnectConfig(s"memory:test-${math.random}", "admin", "admin"))
    }
  }
}
