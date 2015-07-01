package ylabs.orientdb.test

import ylabs.orientdb.DBConfig

trait ODBRemoteTest extends ODBTestBase {

  val dbName: String

  def dbConfig = DBConfig(s"remote:localhost/$dbName", "admin", "admin")

  def dbTestTag = ODBRemoteTestTag

}
