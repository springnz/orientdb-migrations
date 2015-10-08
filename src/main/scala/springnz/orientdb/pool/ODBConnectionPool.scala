package springnz.orientdb.pool

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{LazyLogging, Logger}
import ylabs.util.Pimpers._

import scala.util.Try

trait ODBConnectionPool extends AbstractODBConnectionPool[ODatabaseDocumentTx] with LazyLogging {
  implicit val log = logger

  def dbConfig: Try[ODBConnectConfig]

  def loadDBConfig(path: String)(implicit log: Logger): Try[ODBConnectConfig] =
    Try {
      val config = ConfigFactory.load().getConfig(path)
      val url = Try { config.getString("url") }
        .getOrElse(config.getString("host"))
      val user = config.getString("user")
      val pass = config.getString("pass")
      val connectConfig = ODBConnectConfig(url, user, pass)
      log.info(s"Loaded $connectConfig")
      connectConfig
    }.withErrorLog("loadDBConfig failed")

  lazy val pool: Try[OPartitionedDatabasePool] =
    dbConfig.flatMap(createDatabasePool).withErrorLog("Could not acquire db connection from pool")

  // Creates a pool over database. Database specified in config must exist if it's remote instance.
  // Memory instance is created adhoc.
  def createDatabasePool(config: ODBConnectConfig): Try[OPartitionedDatabasePool] =
    Try {
      val db = new ODatabaseDocumentTx(config.url)
      if (config.url.startsWith("memory:") && !db.exists()) db.create()

      // database need to exist at this stage
      new OPartitionedDatabasePool(config.url, config.user, config.pass)
    }.withErrorLog("Could not create OPartitionedDatabasePool")

  def acquire(): Try[ODatabaseDocumentTx] =
    pool.map(_.acquire()).withErrorLog("Could not acquire db connection from pool")
}

object ODBConnectionPool {

  def fromConfig(path: String) = {
    new ODBConnectionPool {
      override def dbConfig: Try[ODBConnectConfig] = loadDBConfig(path)
    }
  }
}
