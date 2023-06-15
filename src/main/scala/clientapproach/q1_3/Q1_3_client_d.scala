package de.aljoshavieth.redisolapclient
package clientapproach.q1_3

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


object Q1_3_client_d extends RedisQuery {

	/**
	 * Original Q1.3 Query in SQL
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_weeknuminyear = 6
	 * and d_year = 1994
	 * and lo_discount between 5 and 7
	 * and lo_quantity between 26 and 35;
	 */


	override def execute(jedisPooled: JedisPooled): Unit = {
		val dateFilters: List[Query.Filter] = List(new Query.NumericFilter("d_weeknuminyear", 6, 6), new Query.NumericFilter("d_year", 1994, 1994))
		val dateDocuments: List[Document] = queryDocuments(jedisPooled, "date-index", filters = dateFilters, List("d_datekey"))
		//println("found date documents: " + dateDocuments.length)


		val d_datekeys = dateDocuments.flatMap { doc =>
			val validDateRanges = "@lo_orderdate:[" + doc.getString("d_datekey") + " " + doc.getString("d_datekey") + "]"
			List(validDateRanges) // return a List with the dateRange string
		}

		//println(d_datekeys)

		val queryString = d_datekeys.mkString(" | ")

		val reducer: Reducer = Reducers.sum("revenue").as("total_revenue")
		val aggregation = new AggregationBuilder("@lo_discount:[5 7] @lo_quantity:[26 35]" + queryString)
			.load("@lo_discount", "@lo_extendedprice")
			.apply("@lo_discount * @lo_extendedprice", "revenue")
			.groupBy(List.empty[String].asJavaCollection, List(reducer).asJavaCollection)
			.limit(0, Integer.MAX_VALUE) // Optional, set your limit

		val result: AggregationResult = jedisPooled.ftAggregate("lineorder-index", aggregation)
		println("Revenue: " + result.getResults.get(0).get("total_revenue"))
		
	}
}
