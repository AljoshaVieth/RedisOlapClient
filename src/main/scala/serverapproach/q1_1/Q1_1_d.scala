package de.aljoshavieth.redisolapclient
package serverapproach.q1_1

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q1_1_d extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_1_d", List[String]().asJava, List[String]().asJava))
	}
}
