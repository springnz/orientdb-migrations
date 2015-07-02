package ylabs.orientdb.test

import ylabs.orientdb.{ODBConnectConfig, ODBSession}

trait ODBMemoryTest extends ODBTestBase {

  val dbName: String

  def dbConfig = ODBConnectConfig(s"memory:$dbName", "admin", "admin")

  def dbTestTag = ODBMemoryTestTag

  ODBSession(_.create()).run()

}
