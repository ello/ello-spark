package co.ello.impressions

import java.net.URI

object JdbcUrlFromPostgresUrl {
  def apply(url: String): String = {
    val uri = new URI(url)
    uri.getUserInfo() match {
      case str: String => {
        val Array(username, password) = str.split(":")
        s"jdbc:postgresql://${uri.getHost()}:${uri.getPort()}${uri.getPath()}?sslmode=require&user=${username}&password=${password}"
      }
      case _ =>
        s"jdbc:postgresql://${uri.getHost()}:${uri.getPort()}${uri.getPath()}"
    }
  }
}
