package de.aljoshavieth.redisolapclient
package denormalizedapproach

import redis.clients.jedis.JedisPooled

trait RedisearchQuery {
	def execute(jedisPooled: JedisPooled): String
	def isCorrect(result: String): Boolean
}
