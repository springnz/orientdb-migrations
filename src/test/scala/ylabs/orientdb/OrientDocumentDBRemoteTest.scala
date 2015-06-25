package ylabs.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

class OrientDocumentDBRemoteTest extends OrientDocumentDBTest {
  override val db = new ODatabaseDocumentTx("remote:localhost/test")

  override def beforeAll(): Unit = {
    db.open("admin", "admin")
    deleteAndDropClasses()
  }

}
