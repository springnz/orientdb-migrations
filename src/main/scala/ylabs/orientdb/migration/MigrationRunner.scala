package ylabs.orientdb.migration

import ylabs.orientdb.ODBConnectionPool
import ylabs.util.Logging

import scala.util.{Failure, Success, Try}

object MigrationRunner extends App with Logging {

  def getODBConfigPathFromArgs(args: Array[String]): Try[String] =
    args.headOption match {
      case Some(key) ⇒ Success(key)
      case None ⇒
        val msg = "Missing db config argument"
        log.error(msg)
        Failure(new RuntimeException(msg))
    }

  def loadMigrationsFromClasspath(className: String): Try[Seq[Migration]] =
    Try {
      log.info(s"Loading migrations from [$className]")
      val clazz = Class.forName(className)
      val migrations = clazz.newInstance().asInstanceOf[ODBMigrations].migrations
      log.info(s"Found ${migrations.size} migrations")
      migrations
    }

  // sbt "run-main ylabs.orientdb.migration.MigrationRunner db"
  def run(args: Array[String], className: String = "ylabs.orientdb.Migrations"): Try[Unit] = {
    for {
      configPath ← getODBConfigPathFromArgs(args)
      migrations ← loadMigrationsFromClasspath(className)
    } yield {
      val pool = ODBConnectionPool.fromConfig(configPath)
      Migrator.runMigration(migrations)(pool)
    }
  }

  run(args)
}
