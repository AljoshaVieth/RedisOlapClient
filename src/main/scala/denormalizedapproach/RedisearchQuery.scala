package de.aljoshavieth.redisolapclient
package denormalizedapproach

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.search.aggr.AggregationResult

import scala.io.Source

abstract class RedisearchQuery {
	def execute(jedisPooled: JedisPooled): AggregationResult

	def isCorrect(result: String): Boolean

	def toComparableString(results: AggregationResult): String


	def readTextFileIntoString(filename: String): String = {
		val bufferedSource = Source.fromFile(filename)
		val fileContents = bufferedSource.getLines.mkString("\n")
		bufferedSource.close()
		fileContents
	}
}
