package de.aljoshavieth.redisolapclient
package serverapproach.q2_1

import serverapproach.RedisLuaQuery

import redis.clients.jedis.JedisPooled

import scala.jdk.CollectionConverters.*

object Q2_1_server_a extends RedisLuaQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		println(jedisPooled.fcall("runQ2_1_a", List[String]().asJava, List("date-index", "*", "d_datekey", "lo_orderdate", "part-index", "@p_category:{MFGR\\#12}", "p_partkey", "lo_partkey", "supplier-index", "@s_region:{AMERICA}", "s_suppkey", "lo_suppkey").asJava))
	}
}
