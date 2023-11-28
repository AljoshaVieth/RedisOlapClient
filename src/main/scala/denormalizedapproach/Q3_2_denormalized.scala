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


object Q3_2_denormalized extends RedisearchQuery {

	/**
	 * Original SQL Query:
	 *
	 * select c_city, s_city, d_year, sum(lo_revenue) as revenue
	 * from customer, lineorder, supplier, date
	 * where lo_custkey = c_custkey
	 * and lo_suppkey = s_suppkey
	 * and lo_orderdate = d_datekey
	 * and c_nation = 'UNITED STATES'
	 * and s_nation = 'UNITED STATES'
	 * and d_year >= 1992
	 * and d_year <= 1997
	 * group by c_city, s_city, d_year
	 * order by d_year asc, revenue desc;
	 *
	 */


	override def execute(jedisPooled: JedisPooled): AggregationResult = {
		val startTime = System.currentTimeMillis()

		val reducer: Reducer = Reducers.sum("lo_revenue").as("revenue")
		val aggregation = new AggregationBuilder(
			"@c_nation:{UNITED STATES}" +
				" @s_nation:{UNITED STATES}" +
				" @d_year:[1992 1997]")
			.load("c_city", "s_city", "d_year", "lo_revenue")
			.groupBy(List("@c_city", "@s_city", "@d_year").asJavaCollection, List(reducer).asJavaCollection)
			.sortBy(SortedField.asc("@d_year"), SortedField.desc("@revenue"))
			.limit(0, Integer.MAX_VALUE)

		val result = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println("Executed in " + (System.currentTimeMillis() - startTime) + " ms")
		result
	}

	override def isCorrect(result: String): Boolean = {
		readTextFileIntoString("src\\main\\resources\\scale-1\\formattedresults\\q_3_2_result.txt").equals(result)
	}

	override def toComparableString(results: AggregationResult): String = {
		val strings = results.getResults.asScala.map { result =>
			"" + result.get("c_city") + " | " + result.get("s_city") + " | " + result.get("d_year") + " | " + result.get("revenue")
		}
		strings.mkString("\n")
	}
}
