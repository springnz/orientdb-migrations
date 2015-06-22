package ylabs.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import ylabs.logging.Logging
import ylabs.util.Pimpers._

import scala.util.Try

trait OrientDocumentDBConnectionPool extends Logging {

  def loadDBConfig: Option[DBConfig]

  lazy val pool: Option[OPartitionedDatabasePool] =
    loadDBConfig.flatMap(createDatabasePool)

  def createDatabasePool(config: DBConfig): Option[OPartitionedDatabasePool] =
    Try {
      new OPartitionedDatabasePool(config.host, config.user, config.pass)
    }.withErrorLog("Could not create OPartitionedDatabasePool").toOption

  def acquire(): Option[ODatabaseDocumentTx] =
    for {
      p ← pool
      db ← Try(p.acquire()).withErrorLog("Could not acquire db connection from pool").toOption
    } yield db
}
