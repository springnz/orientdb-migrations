package ylabs.orientdb.migration

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import ylabs.orientdb.ODBScala
import ylabs.orientdb.session.ODBSession

class TestMigrations extends ODBMigrations with ODBScala {

  val migration1: ODBSession[Unit] = ODBSession { implicit db ⇒
    val oClass = createClass("Person")
    oClass.createProperty("name", OType.STRING)
  }
  val migration2: ODBSession[Unit] = ODBSession { implicit db ⇒
    val oClass = findClass("Person")
    oClass.createProperty("age", OType.INTEGER)
  }
  val migration3: ODBSession[Unit] = ODBSession { implicit db ⇒
    val doc = new ODocument("Person")
    doc.field("name", "bob")
    doc.field("age", 123)
    doc.save()
  }

  val migrations = Seq(
    Migration(1, migration1),
    Migration(2, migration2),
    Migration(3, migration3))
}
