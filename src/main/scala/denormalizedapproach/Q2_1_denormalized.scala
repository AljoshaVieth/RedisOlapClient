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


object Q2_1_denormalized extends RedisearchQuery {

	/**
	 * Original SQL Query:
	 *
	 * select sum(lo_revenue), d_year, p_brand1
	 * from lineorder, date, part, supplier
	 * where lo_orderdate = d_datekey
	 * and lo_partkey = p_partkey
	 * and lo_suppkey = s_suppkey
	 * and p_category = 'MFGR#12'
	 * and s_region = 'AMERICA'
	 * group by d_year, p_brand1
	 * order by d_year, p_brand1;
	 */


	override def execute(jedisPooled: JedisPooled): AggregationResult = {
		val startTime = System.currentTimeMillis()

		val reducer: Reducer = Reducers.sum("lo_revenue").as("sum")
		val aggregation = new AggregationBuilder("@p_category:{MFGR\\#12} @s_region:{AMERICA}")
			.load("lo_revenue", "d_year", "p_brand1")
			.groupBy(List("@d_year", "@p_brand1").asJavaCollection, List(reducer).asJavaCollection)
			.sortBy(SortedField.asc("@d_year"), SortedField.asc("@p_brand1"))
			.limit(0, Integer.MAX_VALUE)

		val result = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println("Executed in " + (System.currentTimeMillis() - startTime) + " ms")
		result
	}

	override def isCorrect(result: String): Boolean = {
		readTextFileIntoString("src\\main\\resources\\formattedresults\\q_2_1_result.txt").equals(result)
	}


	override def toComparableString(results: AggregationResult): String = {
		val strings = results.getResults.asScala.map { result =>
			"" + result.get("sum") + " | " + result.get("d_year") + " | " + result.get("p_brand1")
		}
		strings.mkString("\n")
	}
}
