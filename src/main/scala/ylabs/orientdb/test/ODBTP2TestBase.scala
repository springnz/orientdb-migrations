package ylabs.orientdb.test

import org.scalatest.Tag
import ylabs.orientdb.ODBScala
import ylabs.orientdb.pool.{ ODBTP2ConnectConfig, ODBTP2ConnectionPool }
import ylabs.util.Logging

import scala.util.{ Success, Try }

trait ODBTP2TestBase extends ODBScala with Logging {

  implicit lazy val pool = new ODBTP2ConnectionPool {
    override def dbConfig: Try[ODBTP2ConnectConfig] = {
      log.info(s"Using $dbConnectConfig")
      Success(dbConnectConfig)
    }
  }

  def dbConnectConfig: ODBTP2ConnectConfig

  def dbTestTag: Tag

  def dbName: String
}
