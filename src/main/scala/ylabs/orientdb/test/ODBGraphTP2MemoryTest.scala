package ylabs.orientdb.test

import ylabs.orientdb.pool.ODBGraphTP2ConnectConfig

trait ODBGraphTP2MemoryTest extends ODBGraphTP2TestBase {

  def dbName: String = "test"

  def dbConnectConfig = ODBGraphTP2ConnectConfig(s"memory:$dbName", "admin", "admin", 1, 20)

  def dbTestTag = ODBMemoryTestTag
}
