package springnz.orientdb.pool

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{ LazyLogging, Logger }
import springnz.orientdb.util.Pimpers._

import scala.util.{ Success, Try }

trait ODBConnectionPool extends AbstractODBConnectionPool[ODatabaseDocumentTx] with LazyLogging {
  implicit val log = logger

  val maxReconnectAttempts = 3
  val reconnectDelaySeconds = 1

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

  private var partitionedDatabasePool: Option[OPartitionedDatabasePool] = None

  private def tryGetOrCreateDatabasePool: Try[OPartitionedDatabasePool] =
    if (partitionedDatabasePool.isDefined)
      Success(partitionedDatabasePool.get)
    else
      dbConfig.flatMap(tryCreateDatabasePool)

  // Creates a pool over database. Database specified in config must exist if it's remote instance.
  // Memory instance is created adhoc.
  private def tryCreateDatabasePool(config: ODBConnectConfig): Try[OPartitionedDatabasePool] =
    Try {
      val db = new ODatabaseDocumentTx(config.url)
      var newPool = new OPartitionedDatabasePool(config.url, config.user, config.pass).setAutoCreate(true)
      partitionedDatabasePool = Some(newPool)
      newPool
    }.withErrorLog("Could not create OPartitionedDatabasePool")

  private def tryAcquireDatabase(partitionedDatabasePool: OPartitionedDatabasePool): Try[ODatabaseDocumentTx] =
    Try {
      partitionedDatabasePool.acquire()
    }.withErrorLog("Could not acquire db connection from pool")

  def acquire(): Try[ODatabaseDocumentTx] = {

    def isDBOnline(tryDB: Try[ODatabaseDocumentTx]) =
      tryDB.isSuccess && tryDB.get.exists()

    var db = tryGetOrCreateDatabasePool.flatMap(tryAcquireDatabase)

    var attempts = 1
    while (!isDBOnline(db) && attempts <= maxReconnectAttempts) {
      attempts += 1
      log.warn(s"Could not connect to database. Delaying ${reconnectDelaySeconds} seconds before attempt $attempts of $maxReconnectAttempts")
      Thread.sleep(reconnectDelaySeconds * 1000)
      partitionedDatabasePool = None
      db = tryGetOrCreateDatabasePool.flatMap(tryAcquireDatabase)
    }

    db
  }
}

object ODBConnectionPool {

  def fromConfig(path: String) = {
    new ODBConnectionPool {
      override def dbConfig: Try[ODBConnectConfig] = loadDBConfig(path)
    }
  }
}

