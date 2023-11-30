package de.aljoshavieth.redisolapclient
package serverapproach.q1_3

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*


object Q1_3_server_a extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_3_a", List[String]().asJava, List("date-index", "@d_year:[1994 1994] @d_weeknuminyear:[6 6]", "d_datekey", "lo_orderdate").asJava))
	}
}
