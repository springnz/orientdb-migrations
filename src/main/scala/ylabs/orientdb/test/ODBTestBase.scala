package ylabs.orientdb.test

import org.scalatest.Tag
import ylabs.orientdb.{ ODBConnectConfig, ODBSession, ODBConnectionPool, ODBScala }
import ylabs.util.Pimpers._

import scala.util.{ Success, Try }

trait ODBTestBase extends ODBScala {

  implicit lazy val pool = new ODBConnectionPool {
    override def loadDBConfig: Try[ODBConnectConfig] = {
      log.info(s"Loading $dbConfig")
      Success(dbConfig)
    }
  }

  def dbConfig: ODBConnectConfig

  def dbTestTag: Tag

  val dbName: String

  val classNames: Seq[String]

  def dropClasses(): Unit = ODBSession { implicit db ⇒
    classNames.foreach(className ⇒ Try { dropClass(className) })
  }.run().withErrorLog("failed to drop classes")

  def deleteClassRecords(): Unit = ODBSession { implicit db ⇒
    classNames.foreach { className ⇒
      Try { sqlCommand(s"delete from $className").execute().asInstanceOf[java.lang.Integer] }
    }
  }.run().withErrorLog("failed to delete class records")
}
