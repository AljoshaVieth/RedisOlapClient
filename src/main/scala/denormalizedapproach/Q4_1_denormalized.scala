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


object Q4_1_denormalized extends RedisearchQuery {

	/**
	 * Original SQL Query:
	 *
	 * select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit
	 * from date, customer, supplier, part, lineorder
	 * where lo_custkey = c_custkey
	 * and lo_suppkey = s_suppkey
	 * and lo_partkey = p_partkey
	 * and lo_orderdate = d_datekey
	 * and c_region = 'AMERICA'
	 * and s_region = 'AMERICA'
	 * and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
	 * group by d_year, c_nation
	 * order by d_year, c_nation;
	 *
	 */


	override def execute(jedisPooled: JedisPooled): AggregationResult = {
		val startTime = System.currentTimeMillis()

		val reducer: Reducer = Reducers.sum("calculated_profit").as("profit")
		val aggregation = new AggregationBuilder(
				"@c_region:{AMERICA}" +
				" @s_region:{AMERICA}" +
				" @p_mfgr:{MFGR\\#1 | MFGR\\#2}")
			.load("c_city", "s_city", "d_year", "lo_revenue", "lo_supplycost")
			.apply("@lo_revenue - @lo_supplycost", "calculated_profit")
			.groupBy(List("@d_year", "@c_nation").asJavaCollection, List(reducer).asJavaCollection)
			.sortBy(SortedField.asc("@d_year"), SortedField.asc("@c_nation"))
			.limit(0, Integer.MAX_VALUE)

		val result = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println("Executed in " + (System.currentTimeMillis() - startTime) + " ms")
		result
	}

	override def isCorrect(result: String): Boolean = {
		readTextFileIntoString("src\\main\\resources\\formattedresults\\q_4_1_result.txt").equals(result)
	}

	override def toComparableString(results: AggregationResult): String = {
		val strings = results.getResults.asScala.map { result =>
			"" + result.get("d_year") + " | " + result.get("c_nation") + " | " + result.get("profit")
		}
		strings.mkString("\n")
	}
}
