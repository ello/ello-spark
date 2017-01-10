package co.ello.testing

import org.scalatest._
import com.redislabs.provider.redis._

trait Redis extends BeforeAndAfterEach { this: Suite =>

  var redisConfig = new RedisConfig(new RedisEndpoint)

  override def beforeEach() {
    redisConfig.hosts.foreach( node => {
      val conn = node.connect
      conn.flushDB
      conn.close
    })
    super.beforeEach() // To be stackable, must call super.beforeEach
  }
}
