package ylabs.orientdb.test

import org.scalatest.Tag
import ylabs.orientdb.{ DBConfig, ODBSession, OrientDocumentDBConnectionPool, OrientDocumentDBScala }
import ylabs.util.Pimpers._

import scala.util.{ Success, Try }

trait ODBTestBase extends OrientDocumentDBScala {

  implicit lazy val pool = new OrientDocumentDBConnectionPool {
    override def loadDBConfig: Try[DBConfig] = {
      log.info(s"Loading $dbConfig")
      Success(dbConfig)
    }
  }

  def dbConfig: DBConfig

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
