package ylabs.orientdb.migration

import java.time.OffsetDateTime

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import ylabs.orientdb.ODBScala._
import ylabs.orientdb.{ ODBConnectionPool, ODBScala, ODBSession }
import ylabs.util.DateTimeUtil
import ylabs.util.Pimpers._

import scala.util.{ Failure, Try }

case class Migration(version: Int, session: ODBSession[Unit])

trait ODBMigrations {
  val migrations: Seq[Migration]
}

case class MigrationLog(version: Int, timestamp: OffsetDateTime) {
  import MigrationLog._
  def toDocument(implicit db: ODatabaseDocumentTx): ODocument = {
    val doc = new ODocument(MigrationLog.className)
    doc.field(versionFieldName, version)
    doc.field(timestampFieldName, timestamp.toLegacyDate)
  }
}

object MigrationLog {
  val className = "MigrationLog"
  val versionFieldName = "version"
  val timestampFieldName = "timestamp"

  def fromDocument(doc: ODocument): MigrationLog = {
    val version = doc.getInt(versionFieldName)
    val timestamp = doc.getUtcOffsetDateTime(timestampFieldName)
    MigrationLog(version, timestamp)
  }
}

object Migrator extends ODBScala {

  def createMigrationLogSchema()(implicit pool: ODBConnectionPool): ODBSession[Unit] =
    ODBSession { implicit db ⇒
      if (getSchema.existsClass(MigrationLog.className)) {
        log.info("MigrationLog schema exists")
      } else {
        log.info("Creating MigrationLog schema")
        val oClass = createClass(MigrationLog.className)
        oClass.setStrictMode(true)
        oClass.createProperty(MigrationLog.versionFieldName, OType.INTEGER).createIndex(INDEX_TYPE.UNIQUE)
        oClass.createProperty(MigrationLog.timestampFieldName, OType.DATETIME)
      }
    }

  def fetchMigrationLogs(implicit pool: ODBConnectionPool): ODBSession[IndexedSeq[MigrationLog]] =
    ODBSession { implicit db ⇒
      log.info("Fetching migration logs...")
      val migrationLogs = selectClass(MigrationLog.className)(MigrationLog.fromDocument)
      migrationLogs.sortBy(_.version)
    }

  def findCurrentSchemaVersion(implicit pool: ODBConnectionPool): ODBSession[Option[Int]] =
    ODBSession { implicit db ⇒
      log.info("Finding current schema version")
      val sql = s"select max(${MigrationLog.versionFieldName}) from ${MigrationLog.className}"
      val result = db.qSingleResult(sql).map(_.getInt("max"))
      result.foreach(version ⇒ log.info(s"Currently at schema version $version"))
      result
    }

  def runMigration(migrations: Seq[Migration])(implicit pool: ODBConnectionPool): Try[Unit] = {

    def isValidMigrationSequence = migrations.map(_.version).distinct.size == migrations.size

    def migrationsToExecute(currentVersion: Option[Int]) = {
      val sortedMigrations = migrations.sortBy(_.version)
      currentVersion match {
        case Some(version) ⇒ sortedMigrations.filter(_.version > version)
        case None          ⇒ sortedMigrations
      }
    }

    def executableMigration(migration: Migration): ODBSession[Int] =
      for {
        _ ← ODBSession(_ ⇒ log.info(s"Migrating to version ${migration.version}..."))
        _ ← migration.session
        _ ← insertLog(migration.version)
      } yield {
        migration.version
      }

    def run(): Try[Unit] = {
      log.info("Starting migration sequence")

      createMigrationLogSchema().run().withErrorLog("Error creating MigrationLog schema")

      val sequence = for {
        currentVersion ← findCurrentSchemaVersion
        migrationResult ← ODBSession.sequence(migrationsToExecute(currentVersion).map(executableMigration))
      } yield {
        log.info(s"Successfully executed all migrations: ${migrationResult.mkString(",")}")
      }

      sequence.run().withErrorLog("Error executing migration")
    }

    def abort: Failure[Unit] = {
      val msg = "Migration sequence aborted since it has duplicate versions. No migrations were executed."
      log.error(msg)
      Failure(new RuntimeException(msg))
    }

    if (isValidMigrationSequence)
      run()
    else
      abort
  }

  def insertLog(version: Int, result: Boolean = true)(implicit pool: ODBConnectionPool): ODBSession[Unit] =
    ODBSession { implicit db ⇒
      log.info(s"Successfully migrated to version $version")
      MigrationLog(version, DateTimeUtil.utcDateTime).toDocument.save()
    }

}
