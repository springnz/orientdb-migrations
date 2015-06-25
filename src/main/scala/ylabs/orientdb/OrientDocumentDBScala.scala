package ylabs.orientdb

import com.orientechnologies.orient.core.command.OCommandRequest
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.{ OClass, OType }
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import ylabs.util.Logging
import ylabs.util.Pimpers._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait OrientDocumentDBScala extends Logging {

  implicit class dbWrapper(db: ODatabaseDocumentTx) {
    def q[T](sql: String, params: AnyRef*): immutable.IndexedSeq[T] = {
      val params4java = params.toArray
      val results: java.util.List[T] = db.query(new OSQLSynchQuery[T](sql), params4java: _*)
      results.asScala.toIndexedSeq
    }
    def count(sql: String, params: AnyRef*): Long = {
      val params4java = params.toArray
      val result: java.util.List[ODocument] = db.query(new OSQLSynchQuery[ODocument](sql), params4java: _*)
      val count: java.lang.Long = result.get(0).field("count")
      count.toLong
    }
  }

  implicit class sqlSynchQueryWrapper[T](sqlSynchQuery: OSQLSynchQuery[T]) {
    def exec(params: AnyRef*)(implicit db: ODatabaseDocumentTx): immutable.IndexedSeq[T] = {
      val params4java = params.toArray
      val results: java.util.List[T] = db.command(sqlSynchQuery).execute(params4java: _*)
      results.asScala.toIndexedSeq
    }
  }

  def getSchema(implicit db: ODatabaseDocumentTx) =
    db.getMetadata.getSchema

  def findClass(className: String)(implicit db: ODatabaseDocumentTx) =
    getSchema.getClass(className)

  def createClass(className: String)(implicit db: ODatabaseDocumentTx) =
    getSchema.createClass(className)

  def dropClass(className: String)(implicit db: ODatabaseDocumentTx) =
    getSchema.dropClass(className)

  def truncateClass(className: String)(implicit db: ODatabaseDocumentTx) =
    findClass(className).truncate()

  def sqlCommand(sql: String)(implicit db: ODatabaseDocumentTx): OCommandRequest =
    db.command(new OCommandSQL(sql))

  def escapeSqlString(string: String) = string.replace("\\", "\\\\").replace("\"", "\\\"")

  def dbFuture[T](block: ⇒ T)(implicit db: ODatabaseDocumentTx, ec: ExecutionContext): Future[T] =
    Future {
      ODatabaseRecordThreadLocal.INSTANCE.set(db)
      block
    }

}
