package ylabs.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

class OrientDocumentDBMemoryTest extends OrientDocumentDBTest {
  override val db = new ODatabaseDocumentTx("memory:doctest")
  db.create()
}



