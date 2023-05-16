package de.aljoshavieth.redisolapclient
package serverapproach

import redis.clients.jedis.JedisPooled
import scala.jdk.CollectionConverters.*


abstract class RedisLuaQuery {
	def execute(jedisPooled: JedisPooled): Unit
}
