package ylabs.orientdb.pool

import com.tinkerpop.blueprints.impls.orient.{ OrientGraph, OrientGraphFactory }
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import ylabs.util.Pimpers._

import scala.util.Try

trait ODBGraphTP2ConnectionPool extends AbstractODBConnectionPool[OrientGraph] with LazyLogging {
  implicit val log = logger

  def dbConfig: Try[ODBGraphConnectConfig]

  def loadDBConfig(path: String)(implicit log: Logger): Try[ODBGraphConnectConfig] =
    Try {
      val config = ConfigFactory.load().getConfig(path)
      val host = config.getString("host")
      val user = config.getString("user")
      val pass = config.getString("pass")
      val minPoolSize = config.getInt("minPoolSize")
      val maxPoolSize = config.getInt("maxPoolSize")
      val connectConfig = ODBGraphConnectConfig(host, user, pass, minPoolSize, maxPoolSize)
      log.info(s"Loaded $connectConfig")
      connectConfig
    }.withErrorLog("loadDBConfig failed")

  lazy val factory: Try[OrientGraphFactory] =
    dbConfig.flatMap(createGraphFactory).withErrorLog("Could not acquire db connection from pool")

  // Creates a pool over database. Database specified in config must exist if it's remote instance.
  // Memory instance is created adhoc.
  def createGraphFactory(config: ODBGraphConnectConfig): Try[OrientGraphFactory] =
    Try {
      new OrientGraphFactory(config.host).setupPool(config.minPoolSize, config.maxPoolSize)
    }.withErrorLog("Could not create OrientGraphFactory")

  def acquire(): Try[OrientGraph] =
    factory.map(_.getTx).withErrorLog("Could not acquire db connection from pool")
}

object ODBGraphTP2ConnectionPool {

  def fromConfig(path: String) = {
    new ODBGraphTP2ConnectionPool {
      override def dbConfig: Try[ODBGraphConnectConfig] = loadDBConfig(path)
    }
  }
}
