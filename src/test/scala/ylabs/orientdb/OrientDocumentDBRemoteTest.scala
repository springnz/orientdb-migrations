package ylabs.orientdb

class OrientDocumentDBRemoteTest extends OrientDocumentDBTest {

  override def dbConfig = DBConfig("remote:localhost/test", "admin", "admin")

}
