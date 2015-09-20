package ylabs.orientdb.test

import ylabs.orientdb.pool.ODBTP2ConnectConfig

trait ODBTP2MemoryTest extends ODBTP2TestBase {

  def dbName: String = "test"

  def dbConnectConfig = ODBTP2ConnectConfig(s"memory:$dbName", "admin", "admin", 1, 20)

  def dbTestTag = ODBMemoryTestTag
}
