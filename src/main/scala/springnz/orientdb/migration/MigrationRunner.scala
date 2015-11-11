package springnz.orientdb.migration

import springnz.orientdb.pool.ODBConnectionPool
import springnz.util.Logging
import springnz.util.Pimpers._

import scala.util.{ Failure, Success, Try }

object MigrationRunner extends App with Logging {

  def getODBConfigPathFromArgs(arg: Option[String]): Try[String] =
    arg match {
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
    }.withErrorLog("Error loading migrations:")

  // sbt "run-main springnz.orientdb.migration.MigrationRunner db springnz.orientdb.Migrations"
  def run(args: Array[String]): Try[Unit] = {
    for {
      configPath ← getODBConfigPathFromArgs(args.headOption)
      migrations ← loadMigrationsFromClasspath(args.lastOption.getOrElse("springnz.orientdb.Migrations"))
    } yield {
      val pool = ODBConnectionPool.fromConfig(configPath)
      Migrator.runMigration(migrations)(pool)
    }
  }

  run(args)
}
