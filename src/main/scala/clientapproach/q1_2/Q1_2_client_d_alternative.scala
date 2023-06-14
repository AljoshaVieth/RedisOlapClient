package de.aljoshavieth.redisolapclient
package clientapproach.q1_2

import clientapproach.RedisQuery
import clientapproach.q1_1.Q1_1_client_a.queryDocuments
import helper.{RedisCommandResponse, RedisCommandResponseBuilder}

import redis.clients.jedis.Protocol.Command
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.search.SearchProtocol.SearchCommand
import redis.clients.jedis.search.aggr.{AggregationBuilder, Reducers}
import redis.clients.jedis.search.{Document, Query, SearchResult}
import redis.clients.jedis.{JedisPooled, Pipeline, Protocol}

import java.nio.charset.StandardCharsets
import java.util
import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*


object Q1_2_client_d_alternative extends RedisQuery {

	/**
	 * Original Q1.2 Query in SQL
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_yearmonthnum = 199401
	 * and lo_discount between 4 and 6
	 * and lo_quantity between 26 and 35;
	 */


	override def execute(jedisPooled: JedisPooled): Unit = {
		val dateFilters: List[Query.Filter] = List(new Query.NumericFilter("d_yearmonthnum", 199401, 199401))
		val dateDocuments: List[Document] = queryDocuments(jedisPooled, "date-index", filters = dateFilters, List("d_datekey"))
		//println("found date documents: " + dateDocuments.length)


		val d_datekeys = dateDocuments.flatMap { doc =>
			val validDateRanges = "@lo_orderdate:[" + doc.getString("d_datekey") + " " + doc.getString("d_datekey") + "]"
			List(validDateRanges) // return a List with the dateRange string
		}

		//println(d_datekeys)

		val queryString = d_datekeys.mkString(" | ")


		/*
		// It is not possble to user GROUPBY 0 with the aggregation builder...
		// Construct the aggregation
		val aggregation = new AggregationBuilder()
			.load("@lo_discount", "@lo_extendedprice")
			.apply("@lo_discount * @lo_extendedprice", "revenue")
			.groupBy("", Reducers.sum("revenue").as("total_revenue"))
			.limit(0, Integer.MAX_VALUE) // Optional, set your limit

		val res = jedisPooled.ftAggregate("lineorder-index", aggregation)

		// Process your results here
		val rows = res.getRows
		rows.forEach { row =>
			println(s"Revenue: ${row.get("revenue")}, Total revenue: ${row.get("total_revenue")}")
		}
		*/
		val response = jedisPooled.sendCommand(SearchCommand.AGGREGATE, "lineorder-index", "@lo_discount:[4 6] @lo_quantity:[26 35]" + queryString, "LOAD", "2", "@lo_discount", "@lo_extendedprice", "APPLY", "@lo_discount * @lo_extendedprice", "AS", "revenue", "GROUPBY", "0", "REDUCE", "SUM", "1", "@revenue", "AS", "total_revenue")

		val startTime = System.currentTimeMillis()
		val redisCommandResponse: RedisCommandResponse = RedisCommandResponseBuilder.buildRedisCommandResponse(response)
		println("  (Needed " + (System.currentTimeMillis() - startTime) + "ms to deconstruct RedisCommandResponse)")
		println("Revenue: " + redisCommandResponse.values(1))
	}
}
