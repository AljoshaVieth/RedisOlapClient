package de.aljoshavieth.redisolapclient
package serverapproach.q1_1

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q1_1_e extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ1_1_e", List[String]().asJava, List[String]().asJava))
	}
}
