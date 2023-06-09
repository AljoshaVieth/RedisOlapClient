package de.aljoshavieth.redisolapclient
package serverapproach.q1_2

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q1_2_server_d extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_2_d", List[String]().asJava, List("date-index", "@d_yearmonthnum:[199401 199401]", "d_datekey", "lo_orderdate").asJava))
	}
}
