package ylabs.orientdb.test

import ylabs.orientdb.pool.ODBConnectConfig
import ylabs.orientdb.session.ODBSession

trait ODBMemoryTest extends ODBTestBase {

  def dbName: String

  def dbConnectConfig = ODBConnectConfig(s"memory:$dbName", "admin", "admin")

  def dbTestTag = ODBMemoryTestTag

  ODBSession(_.create()).run()

}
