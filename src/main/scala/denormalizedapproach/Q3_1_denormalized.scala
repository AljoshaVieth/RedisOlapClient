package de.aljoshavieth.redisolapclient
package denormalizedapproach

import clientapproach.RedisQuery
import clientapproach.q1_1.Q1_1_client_a.queryDocuments
import helper.{RedisCommandResponse, RedisCommandResponseBuilder}

import redis.clients.jedis.Protocol.Command
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.search.SearchProtocol.SearchCommand
import redis.clients.jedis.search.aggr.*
import redis.clients.jedis.search.{Document, Query, SearchResult}
import redis.clients.jedis.{JedisPooled, Pipeline, Protocol}

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.immutable.List
import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*


object Q3_1_denormalized extends RedisearchQuery {

	/**
	 * Original SQL Query:
	 *
	 * select c_nation, s_nation, d_year, sum(lo_revenue) as revenue
	 * from customer, lineorder, supplier, date
	 * where lo_custkey = c_custkey
	 * and lo_suppkey = s_suppkey
	 * and lo_orderdate = d_datekey
	 * and c_region = 'ASIA'
	 * and s_region = 'ASIA'
	 * and d_year >= 1992
	 * and d_year <= 1997
	 * group by c_nation, s_nation, d_year
	 * order by d_year asc, revenue desc;
	 *
	 *
	 */


	override def execute(jedisPooled: JedisPooled): String = {
		val reducer: Reducer = Reducers.sum("lo_revenue").as("revenue")
		val aggregation = new AggregationBuilder(
				"@c_region:{ASIA}" +
				" @s_region:{ASIA}" +
				" @d_year:[1992 1997]")
			.load("c_nation", "s_nation", "d_year", "lo_revenue")
			.groupBy(List("@c_nation", "@s_nation", "@d_year").asJavaCollection, List(reducer).asJavaCollection)
			.sortBy(SortedField.asc("@d_year"), SortedField.desc("@revenue"))
			.limit(0, Integer.MAX_VALUE)

		val result: AggregationResult = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println(result.getTotalResults + " results:")
		println(result.getResults.forEach(x => println(x)))
		""
	}

	override def isCorrect(result: String): Boolean = {
		result.equals("77971813568")
	}
}
