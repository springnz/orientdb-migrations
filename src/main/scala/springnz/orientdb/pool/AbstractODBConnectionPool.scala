package springnz.orientdb.pool

import scala.util.Try

abstract class AbstractODBConnectionPool[Database] {
   def acquire(): Try[Database]
 }
