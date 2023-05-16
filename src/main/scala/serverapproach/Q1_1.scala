package de.aljoshavieth.redisolapclient
package serverapproach

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q1_1 extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_1", List[String]().asJava, List("date-index", "@d_year:[1993 1993]", "d_datekey", "lo_orderdate").asJava))
	}
}
