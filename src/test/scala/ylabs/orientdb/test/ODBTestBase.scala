package ylabs.orientdb.test

import org.scalatest.Tag
import ylabs.orientdb.{DBConfig, OrientDocumentDBConnectionPool}

import scala.util.{Success, Try}

trait ODBTestBase {

  implicit lazy val pool = new OrientDocumentDBConnectionPool {
    override def loadDBConfig: Try[DBConfig] = {
      log.info(s"Loading $dbConfig")
      Success(dbConfig)
    }
  }

  def dbConfig: DBConfig

  def defaultTestTag: Tag

  val dbName: String
}
