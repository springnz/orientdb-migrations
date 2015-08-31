package ylabs.orientdb.session

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

final case class ODBSession[+A](override val block: ODatabaseDocumentTx ⇒ A)
    extends AbstractODBSession[A, ODatabaseDocumentTx](block) {

  def run(db: ODatabaseDocumentTx): A = {
    ODatabaseRecordThreadLocal.INSTANCE.set(db)
    val result = block(db)
    close(db)
    result
  }

  def close(db: ODatabaseDocumentTx): Unit =
    if (!db.isClosed) db.close()
}

object ODBSession extends ODBSessionInstances[ODatabaseDocumentTx, ODBSession]
