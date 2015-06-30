package ylabs.orientdb.test

import ylabs.orientdb.{DBConfig, OrientDbSession}

trait ODBMemoryTest extends ODBTestBase {

  val dbName: String

  def dbConfig = DBConfig(s"memory:$dbName", "admin", "admin")

  def defaultTestTag = ODBMemoryTestTag

  OrientDbSession(_.create()).run()

}
