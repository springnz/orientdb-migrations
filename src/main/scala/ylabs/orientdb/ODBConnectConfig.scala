package ylabs.orientdb

case class ODBConnectConfig(host: String, user: String, pass: String) {
  override def toString = s"ODBConnectConfig(host=$host, user=$user)"
}
