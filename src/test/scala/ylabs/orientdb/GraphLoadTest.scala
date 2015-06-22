package ylabs.orientdb

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import collection.JavaConversions._
import concurrent.ExecutionContext.Implicits.global
import collection.mutable
import com.tinkerpop.blueprints.impls.orient._
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion

class GraphLoadTest extends WordSpec with ShouldMatchers {
  // import OrientDBScala._

  // first need to run the following with console.sh:
  // CREATE DATABASE remote:localhost/graphtest root root plocal graph

  val path = s"graphtest-${math.random}".substring(0, 20)
  val graphFactory = new OrientGraphFactory(s"plocal:target/$path")
  // val graphFactory = new OrientGraphFactory("memory:test")
  // val graphFactory = new OrientGraphFactory("remote:localhost/graphtest")
  val graph = graphFactory.setupPool(1, 10)
    // .getTx
    .getNoTx

  object Labels extends Enumeration {
    val Listing, User, Session, HadSession, ViewListing, ViewNumber = Value
  }
  import Labels._
  type Id = String

  val ids = mutable.Map.empty[Labels.Value, Seq[Id]].withDefaultValue(Seq.empty)

  def createVertices(count: Int, label: Labels.Value)(properties: ⇒ Map[String, AnyRef]): Unit = {
    (1 to count) foreach { i ⇒
      if (i % 1000 == 0) println(s"$label: $i/$count")
      val props = properties + ("lbl" → label.toString)
      // blueprints api expect tuples of key/value in one list...
      val propsList = props.map { case (key, value) ⇒ Seq(key, value) }.flatten.toSeq
      val v = graph.addVertex(null, propsList: _*)
      ids.update(label, ids(label) :+ v.getId.toString)
      // if (i % 1000 == 0) graph.stopTransaction(Conclusion.SUCCESS)
    }
  }

  def randomVertex(label: Labels.Value) = {
    val list = ids(label)
    val number = math.random * list.size
    graph.getVertex(list(number.toInt))
  }

  def addSessions(count: Int) =
    (1 to count) foreach { i ⇒
      if (i % 1000 == 0) println(s"Session: $i/$count")
      val user = randomVertex(User)
      val session = graph.addVertex(null, "lbl", Session.toString)
      ids.update(Session, ids(Session) :+ session.getId.toString)

      graph.addEdge(null, user, session, HadSession.toString)

      //TODO: add a random property to this, e.g. the time
    }

  def createListingViews(count: Int) =
    (1 to count) foreach { i ⇒
      if (i % 1000 == 0) println(s"ListingView: $i/$count")
      val session = randomVertex(Session)
      val listing = randomVertex(Listing)
      graph.addEdge(null, session, listing, ViewListing.toString)
      //TODO: add a random property to this, e.g. the time
    }

  def createViewNumberEvents(count: Int) =
    (1 to count) foreach { i ⇒
      if (i % 1000 == 0) println(s"ViewNumberEvent: $i/$count")
      val session = randomVertex(Session)
      val listing = randomVertex(Listing)
      graph.addEdge(null, session, listing, ViewNumber.toString)
      //TODO: add a random property to this, e.g. the time
    }

  "tinkerpop api" should {
    "create scenario graph" in {
      println("starting to delete the existing elements")
      graph.getEdges.foreach(_.remove())
      graph.getVertices.foreach(_.remove())
      println("deleted all elements")

      val users = 100
      val listings = 100
      time {
        println("creating users")
        createVertices(users, User) { Map("gaId" → s"gaId_${math.random}") }
        println("creating sessions")
        addSessions(users * 3)
        println("creating listings")
        createVertices(listings, Listing) { Map("url" → s"http://us.co.nz/${math.random}") }
        println("creating views")
        createListingViews(users * 6)
        println("creating viewNumber events")
        createViewNumberEvents(users * 2)
      }

      println("vertex count: " + graph.getVertices.size)
      println("edge count: " + graph.getEdges.size)
      println(s"database name: $path")
    }
  }

  def numberBetween(lower: Int, upper: Int): Int = (math.random * upper - lower).toInt

  def time[R](block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }
}
