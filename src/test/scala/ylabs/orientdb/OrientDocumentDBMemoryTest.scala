package ylabs.orientdb

class OrientDocumentDBMemoryTest extends OrientDocumentDBTest {

  override def dbConfig = DBConfig("memory:doctest", "admin", "admin")

  def createDatabase(): Unit = OrientDbSession(_.create()).run()

  override def beforeAll(): Unit = {
    createDatabase()
    super.beforeAll()
  }
}

