package ylabs.orientdb

import com.orientechnologies.orient.core.command.OCommandRequest
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.{OClass, OType}
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import ylabs.logging.Logging

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait OrientDocumentDBScala extends Logging {

  implicit class dbWrapper[A](db: ODatabaseDocumentTx) {
    def q[T](sql: String, params: AnyRef*): immutable.IndexedSeq[T] = {
      val params4java = params.toArray
      val results: java.util.List[T] = db.query(new OSQLSynchQuery[T](sql), params4java: _*)
      results.asScala.toIndexedSeq
    }
  }

  def getSchema = ODatabaseRecordThreadLocal.INSTANCE.get.getMetadata.getSchema

  def createClass(className: String)(implicit db: ODatabaseDocumentTx) =
    db.getMetadata.getSchema.createClass(className)

  def createProperty(oClass: OClass, propertyName: String, oType: OType) =
    oClass.createProperty(propertyName, oType)

  def createIndex(
    className: String,
    propertyName: String,
    oType: OType, indexType: INDEX_TYPE)(
      implicit db: ODatabaseDocumentTx) =
    createClass(className).createProperty(propertyName, oType).createIndex(indexType)

  def sqlCommand(sql: String)(implicit db: ODatabaseDocumentTx): OCommandRequest =
    db.command(new OCommandSQL(sql))

  def dropClass(className: String)(implicit db: ODatabaseDocumentTx) =
    db.getMetadata.getSchema.dropClass(className)

  def dbFuture[T](block: ⇒ T)(implicit db: ODatabaseDocumentTx, ec: ExecutionContext): Future[T] =
    Future {
      ODatabaseRecordThreadLocal.INSTANCE.set(db)
      block
    }

  def closeDb(db: ODatabaseDocumentTx): Boolean =
    Try {
      db.close()
    }.withErrorLog("db.close() failed").isSuccess

  // Acquires a DB instance, executes the block, then closes the DB instance.
  def managedDBExecution[T](pool: OrientDocumentDBConnectionPool, errorHint: String)(
    block: ODatabaseDocumentTx ⇒ T) = {

    def doDBExecution(db: ODatabaseDocumentTx): Option[T] =
      Try {
        block(db)
      }.withErrorLog(errorHint).withFinally(db.close).toOption

    for {
      db ← pool.acquire()
      result ← doDBExecution(db)
    } yield {
      result
    }
  }
}
