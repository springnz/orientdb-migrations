package ylabs.orientdb.test

import org.scalatest.Tag
import ylabs.orientdb.ODBScala
import ylabs.orientdb.pool.{ ODBGraphConnectConfig, ODBGraphTP2ConnectionPool }
import ylabs.util.Logging

import scala.util.{ Success, Try }

trait ODBGraphTP2TestBase extends ODBScala with Logging {

  implicit lazy val pool = new ODBGraphTP2ConnectionPool {
    override def dbConfig: Try[ODBGraphConnectConfig] = {
      log.info(s"Using $dbConnectConfig")
      Success(dbConnectConfig)
    }
  }

  def dbConnectConfig: ODBGraphConnectConfig

  def dbTestTag: Tag

  def dbName: String
}
