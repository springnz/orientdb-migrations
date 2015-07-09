package ylabs.orientdb.test

import ylabs.orientdb.{ODBConnectConfig, ODBSession}

trait ODBMemoryTest extends ODBTestBase {

  def dbName: String

  def dbConnectConfig = ODBConnectConfig(s"memory:$dbName", "admin", "admin")

  def dbTestTag = ODBMemoryTestTag

  ODBSession(_.create()).run()

}
