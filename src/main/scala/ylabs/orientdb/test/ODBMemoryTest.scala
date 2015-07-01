package ylabs.orientdb.test

import ylabs.orientdb.{DBConfig, ODBSession}

trait ODBMemoryTest extends ODBTestBase {

  val dbName: String

  def dbConfig = DBConfig(s"memory:$dbName", "admin", "admin")

  def dbTestTag = ODBMemoryTestTag

  ODBSession(_.create()).run()

}
