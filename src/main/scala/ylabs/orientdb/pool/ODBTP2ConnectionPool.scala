package ylabs.orientdb.pool

import com.tinkerpop.blueprints.impls.orient.{ OrientGraph, OrientGraphFactory }
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import ylabs.util.Pimpers._

import scala.util.Try

trait ODBTP2ConnectionPool extends AbstractODBConnectionPool[OrientGraph] with LazyLogging {
  implicit val log = logger

  def dbConfig: Try[ODBTP2ConnectConfig]

  def loadDBConfig(path: String)(implicit log: Logger): Try[ODBTP2ConnectConfig] =
    Try {
      val config = ConfigFactory.load().getConfig(path)
      val host = config.getString("host")
      val user = config.getString("user")
      val pass = config.getString("pass")
      val minPoolSize = config.getInt("minPoolSize")
      val maxPoolSize = config.getInt("maxPoolSize")
      val connectConfig = ODBTP2ConnectConfig(host, user, pass, minPoolSize, maxPoolSize)
      log.info(s"Loaded $connectConfig")
      connectConfig
    }.withErrorLog("loadDBConfig failed")

  lazy val factory: Try[OrientGraphFactory] =
    dbConfig.flatMap(createGraphFactory).withErrorLog("Could not acquire db connection from pool")

  // Creates a pool over database. Database specified in config must exist if it's remote instance.
  // Memory instance is created adhoc.
  def createGraphFactory(config: ODBTP2ConnectConfig): Try[OrientGraphFactory] =
    Try {
      new OrientGraphFactory(config.host).setupPool(config.minPoolSize, config.maxPoolSize)
    }.withErrorLog("Could not create OrientGraphFactory")

  def acquire(): Try[OrientGraph] =
    factory.map(_.getTx).withErrorLog("Could not acquire db connection from pool")
}

object ODBTP2ConnectionPool {

  def fromConfig(path: String) = {
    new ODBTP2ConnectionPool {
      override def dbConfig: Try[ODBTP2ConnectConfig] = loadDBConfig(path)
    }
  }
}
