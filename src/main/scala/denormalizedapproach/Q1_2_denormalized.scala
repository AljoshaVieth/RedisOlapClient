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
import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*


object Q1_2_denormalized extends RedisearchQuery {

	/**
	 * Original Query in SQL:
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_yearmonthnum = 199401
	 * and lo_discount between 4 and 6
	 * and lo_quantity between 26 and 35;
	 */


	override def execute(jedisPooled: JedisPooled): AggregationResult = {
		val startTime = System.currentTimeMillis()

		val reducer: Reducer = Reducers.sum("revenue").as("total_revenue")
		val aggregation = new AggregationBuilder("@lo_discount:[4 6] @lo_quantity:[26 35] @lo_orderdate:[19940101 19940131]")
			.load("@lo_discount", "@lo_extendedprice")
			.apply("@lo_discount * @lo_extendedprice", "revenue")
			.groupBy(List.empty[String].asJavaCollection, List(reducer).asJavaCollection)
			.limit(0, Integer.MAX_VALUE)

		val result = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println("Executed in " + (System.currentTimeMillis() - startTime) + " ms")
		result
	}

	override def isCorrect(result: String): Boolean = {
		readTextFileIntoString("src\\main\\resources\\formattedresults\\q_1_2_result.txt").equals(result)
	}


	override def toComparableString(results: AggregationResult): String = {
		results.getResults.get(0).get("total_revenue").toString
	}
}
