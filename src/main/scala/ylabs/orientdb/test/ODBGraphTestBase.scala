package ylabs.orientdb.test

import org.scalatest.Tag
import ylabs.orientdb.ODBScala
import ylabs.orientdb.pool.{ ODBGraphConnectConfig, ODBGraphConnectionPool }
import ylabs.util.Logging

import scala.util.{ Success, Try }

trait ODBGraphTestBase extends ODBScala with Logging {

  implicit lazy val pool = new ODBGraphConnectionPool {
    override def dbConfig: Try[ODBGraphConnectConfig] = {
      log.info(s"Using $dbConnectConfig")
      Success(dbConnectConfig)
    }
  }

  def dbConnectConfig: ODBGraphConnectConfig

  def dbTestTag: Tag

  def dbName: String
}
