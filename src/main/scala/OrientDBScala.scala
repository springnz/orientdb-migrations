
import com.orientechnologies.orient.core.db.record.ODatabaseRecord
import com.orientechnologies.orient.core.db.{ODatabaseComplex, ODatabaseRecordThreadLocal}
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.{OClass, OType}
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object OrientDBScala {

  implicit def dbWrapper[A](db: ODatabaseComplex[A]) = new {
    def q[T](sql: String, params: AnyRef*): List[T] = {
      val params4java = params.toArray
      val results: java.util.List[T] = db.query(new OSQLSynchQuery[T](sql), params4java: _*)
      results.asScala.toList
    }
  }

  def getSchema = ODatabaseRecordThreadLocal.INSTANCE.get.getMetadata.getSchema

  def createClass(className: String)(implicit db: ODatabaseRecord) =
    db.getMetadata.getSchema.createClass(className)

  def createProperty(oClass: OClass, propertyName: String, oType: OType) =
    oClass.createProperty(propertyName, oType)

  def createIndex(
    className: String,
    propertyName: String,
    oType: OType, indexType: INDEX_TYPE)(
      implicit db: ODatabaseRecord) =
    createClass(className).createProperty(propertyName, oType).createIndex(indexType)

  def sqlCommand(sql: String)(implicit db: ODatabaseRecord): OCommandSQL =
    db.command(new OCommandSQL(sql))

  def dropClass(className: String)(implicit db: ODatabaseRecord) =
    db.getMetadata.getSchema.dropClass(className)

  def dbFuture[T](block: ⇒ T)(implicit db: ODatabaseRecord, ec: ExecutionContext): Future[T] =
    Future {
      ODatabaseRecordThreadLocal.INSTANCE.set(db)
      implicit val _db = db
      block
    }

}
