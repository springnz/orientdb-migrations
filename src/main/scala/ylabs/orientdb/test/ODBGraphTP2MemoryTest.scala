package ylabs.orientdb.test

import ylabs.orientdb.pool.ODBGraphConnectConfig

trait ODBGraphTP2MemoryTest extends ODBGraphTP2TestBase {

  def dbName: String = "test"

  def dbConnectConfig = ODBGraphConnectConfig(s"memory:$dbName", "admin", "admin", 1, 20)

  def dbTestTag = ODBMemoryTestTag
}
