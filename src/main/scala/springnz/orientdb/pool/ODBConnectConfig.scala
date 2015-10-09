package springnz.orientdb.pool

case class ODBConnectConfig(
    url: String, user: String, pass: String) {
  override def toString = s"ODBConnectConfig(url=$url, user=$user)"
}


