package springnz.orientdb.test

import springnz.orientdb.pool.ODBConnectConfig
import springnz.orientdb.session.ODBSession

trait ODBMemoryTest extends ODBTestBase {

  def dbName: String

  def dbConnectConfig = ODBConnectConfig(s"memory:$dbName", "admin", "admin")

  def dbTestTag = ODBMemoryTestTag

  ODBSession(_.create()).run()

}
