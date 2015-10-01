package ylabs.orientdb.pool

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import gremlin.scala.ScalaGraph
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import ylabs.util.Pimpers._

import scala.util.Try

trait ODBGremlinConnectionPool extends AbstractODBConnectionPool[ScalaGraph[OrientGraph]] with LazyLogging {
  implicit val log = logger

  def dbConfig: Try[ODBConnectConfig]

  def loadDBConfig(path: String)(implicit log: Logger): Try[ODBConnectConfig] =
    Try {
      val config = ConfigFactory.load().getConfig(path)
      val host = config.getString("host")
      val user = config.getString("user")
      val pass = config.getString("pass")
      val connectConfig = ODBConnectConfig(host, user, pass)
      log.info(s"Loaded $connectConfig")
      connectConfig
    }.withErrorLog("loadDBConfig failed")

  lazy val factory: Try[OrientGraphFactory] =
    dbConfig.flatMap(createGraphFactory).withErrorLog("Could not acquire db connection from pool")

  // Creates a pool over database. Database specified in config must exist if it's remote instance.
  // Memory instance is created adhoc.
  def createGraphFactory(config: ODBConnectConfig): Try[OrientGraphFactory] =
    Try {
      new OrientGraphFactory(config.host)
    }.withErrorLog("Could not create OrientGraphFactory")

  def acquire(): Try[ScalaGraph[OrientGraph]] = {
    factory.map(_.getNoTx).map(ScalaGraph.apply).withErrorLog("Could not acquire db connection from pool")
  }
}

object ODBGremlinConnectionPool {

  def fromConfig(path: String) = {
    new ODBGremlinConnectionPool {
      override def dbConfig: Try[ODBConnectConfig] = loadDBConfig(path)
    }
  }
}
