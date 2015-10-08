package springnz.orientdb.test

import springnz.orientdb.pool.ODBConnectConfig

trait ODBRemoteTest extends ODBTestBase {

  val dbName: String

  def dbConnectConfig = ODBConnectConfig(s"remote:localhost/$dbName", "admin", "admin")

  def dbTestTag = ODBRemoteTestTag

}
