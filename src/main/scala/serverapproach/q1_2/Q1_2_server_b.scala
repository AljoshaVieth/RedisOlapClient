package de.aljoshavieth.redisolapclient
package serverapproach.q1_2

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q1_2_server_b extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_2_b", List[String]().asJava, List[String]().asJava))
	}
}
