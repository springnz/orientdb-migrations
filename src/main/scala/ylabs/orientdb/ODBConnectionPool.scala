package ylabs.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.typesafe.scalalogging.LazyLogging
import ylabs.util.Pimpers._

import scala.util.Try

trait ODBConnectionPool extends LazyLogging {
  implicit val log = logger

  def loadDBConfig: Try[ODBConnectConfig]

  lazy val pool: Try[OPartitionedDatabasePool] =
    loadDBConfig.flatMap(createDatabasePool).withErrorLog("Could not acquire db connection from pool")

  def createDatabasePool(config: ODBConnectConfig): Try[OPartitionedDatabasePool] =
    Try {
      new OPartitionedDatabasePool(config.host, config.user, config.pass)
    }.withErrorLog("Could not create OPartitionedDatabasePool")

  def acquire(): Try[ODatabaseDocumentTx] =
    pool.map(_.acquire()).withErrorLog("Could not acquire db connection from pool")
}
