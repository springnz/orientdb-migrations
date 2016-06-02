package springnz.orientdb.pool

import org.scalatest._
import springnz.orientdb.session.ODBSession
import springnz.orientdb.test.ODBRemoteTest
import springnz.orientdb.util.Pimpers._

@Ignore
class ConnectionPoolReconnectManualTest
    extends WordSpec with ShouldMatchers
    with ODBRemoteTest {

  override def classNames: Seq[String] = Seq.empty

  // create database remote:localhost:2424/test root root plocal document
  override val dbName: String = "remote:localhost/test"

  "attempt reconnection" in {
    Stream.from(0).foreach { i ⇒
      log.info(s"Quering orientdb (iteration $i)")
      ODBSession { implicit db ⇒
        val count = db.count("select count(*) from OUser")
        log.info(s"Successfully retrieved value from orientdb ('select count(*) from OUser' returned $count)")
      }.run().withErrorLog("Error occurred when accessing orientdb")
      Thread.sleep(1000)
    }
  }
}
