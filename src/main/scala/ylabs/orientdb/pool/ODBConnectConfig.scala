package ylabs.orientdb.pool

case class ODBConnectConfig(
    host: String, user: String, pass: String) {
  override def toString = s"ODBConnectConfig(host=$host, user=$user)"
}

case class ODBTP2ConnectConfig(
  host: String, user: String, pass: String, minPoolSize: Int, maxPoolSize: Int) {
  override def toString = s"ODBGraphTP2ConnectConfig(host=$host, user=$user, minPoolSize=$minPoolSize, maxPoolSize=$maxPoolSize)"
}
