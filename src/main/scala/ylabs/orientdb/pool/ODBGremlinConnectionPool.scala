package ylabs.orientdb.pool

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import gremlin.scala.ScalaGraph
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory
import ylabs.util.Pimpers._

import scala.util.Try

trait ODBGremlinConnectionPool extends AbstractODBConnectionPool[ScalaGraph] with LazyLogging {
  implicit val log = logger

  def dbConfig: Try[ODBGremlinConnectConfig]

  def loadDBConfig(path: String)(implicit log: Logger): Try[ODBGremlinConnectConfig] =
    Try {
      val config = ConfigFactory.load().getConfig(path)
      val host = config.getString("host")
      val user = config.getString("user")
      val pass = config.getString("pass")
      val connectConfig = ODBGremlinConnectConfig(host, user, pass)
      log.info(s"Loaded $connectConfig")
      connectConfig
    }.withErrorLog("loadDBConfig failed")

  lazy val factory: Try[OrientGraphFactory] =
    dbConfig.flatMap(createGraphFactory).withErrorLog("Could not acquire db connection from pool")

  // Creates a pool over database. Database specified in config must exist if it's remote instance.
  // Memory instance is created adhoc.
  def createGraphFactory(config: ODBGremlinConnectConfig): Try[OrientGraphFactory] =
    Try {
      new OrientGraphFactory(config.host)
    }.withErrorLog("Could not create OrientGraphFactory")

  def acquire(): Try[ScalaGraph] = {
    factory.map(_.getNoTx).map(ScalaGraph.apply).withErrorLog("Could not acquire db connection from pool")
  }
}

object ODBGremlinConnectionPool {

  def fromConfig(path: String) = {
    new ODBGremlinConnectionPool {
      override def dbConfig: Try[ODBGremlinConnectConfig] = loadDBConfig(path)
    }
  }
}
