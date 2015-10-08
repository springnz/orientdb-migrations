package springnz.orientdb.pool

case class ODBConnectConfig(
    url: String, user: String, pass: String) {
  override def toString = s"ODBConnectConfig(url=$url, user=$user)"
}

case class ODBTP2ConnectConfig(
  url: String, user: String, pass: String, minPoolSize: Int, maxPoolSize: Int) {
  override def toString = s"ODBGraphTP2ConnectConfig(url=$url, user=$user, minPoolSize=$minPoolSize, maxPoolSize=$maxPoolSize)"
}
