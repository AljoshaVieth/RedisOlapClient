package de.aljoshavieth.redisolapclient
package serverapproach.q1_3

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q1_3 extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_2", List[String]().asJava, List("date-index", "@d_yearmonthnum:[199401 199401]", "d_datekey", "lo_orderdate").asJava))
	}
}
