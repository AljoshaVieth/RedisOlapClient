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


object Q4_3_denormalized extends RedisearchQuery {

	/**
	 * Original SQL Query:
	 *
	 * select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as profit
	 * from date, customer, supplier, part, lineorder
	 * where lo_custkey = c_custkey
	 * and lo_suppkey = s_suppkey
	 * and lo_partkey = p_partkey
	 * and lo_orderdate = d_datekey
	 * and c_region = 'AMERICA'
	 * and s_nation = 'UNITED STATES'
	 * and (d_year = 1997 or d_year = 1998)
	 * and p_category = 'MFGR#14'
	 * group by d_year, s_city, p_brand1
	 * order by d_year, s_city, p_brand1;
	 *
	 */


	override def execute(jedisPooled: JedisPooled): AggregationResult = {
		val startTime = System.currentTimeMillis()

		val reducer: Reducer = Reducers.sum("calculated_profit").as("profit")
		val aggregation = new AggregationBuilder(
			"@c_region:{AMERICA}" +
				" @s_nation:{UNITED STATES}" +
				" @d_year:[1997 1997] | @d_year:[1998 1998]" +
				" @p_category:{MFGR\\#14}")
			.load("s_city", "d_year", "lo_revenue", "lo_supplycost", "p_brand1")
			.apply("@lo_revenue - @lo_supplycost", "calculated_profit")
			.groupBy(List("@d_year", "@s_city", "@p_brand1").asJavaCollection, List(reducer).asJavaCollection)
			.sortBy(SortedField.asc("@d_year"), SortedField.asc("@s_city"), SortedField.asc("@p_brand1"))
			.limit(0, Integer.MAX_VALUE)
		val result = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println("Executed in " + (System.currentTimeMillis() - startTime) + " ms")
		result	}

	override def isCorrect(result: String): Boolean = {
		readTextFileIntoString("src\\main\\resources\\scale-1\\formattedresults\\q_4_3_result.txt").equals(result)
	}

	override def toComparableString(results: AggregationResult): String = {
		val strings = results.getResults.asScala.map { result =>
			"" + result.get("d_year") + " | " + result.get("s_city") + " | " + result.get("p_brand1") + " | " + result.get("profit")
		}
		strings.mkString("\n")
	}
}
