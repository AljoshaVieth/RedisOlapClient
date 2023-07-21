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
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*


object Q1_3_denormalized extends RedisearchQuery {

	/**
	 * Original Query in SQL:
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_weeknuminyear = 6
	 * and d_year = 1994
	 * and lo_discount between 5 and 7
	 * and lo_quantity between 26 and 35;
	 */


	override def execute(jedisPooled: JedisPooled): AggregationResult = {
		val startTime = System.currentTimeMillis()

		val reducer: Reducer = Reducers.sum("revenue").as("total_revenue")
		val aggregation = new AggregationBuilder("@lo_discount:[5 7] @lo_quantity:[26 35] @lo_orderdate:" + getDateRangePerYearAndWeeknumber(1994, 6))
			.load("@lo_discount", "@lo_extendedprice")
			.apply("@lo_discount * @lo_extendedprice", "revenue")
			.groupBy(List.empty[String].asJavaCollection, List(reducer).asJavaCollection)
			.limit(0, Integer.MAX_VALUE)

		val result = jedisPooled.ftAggregate("denormalized-index", aggregation)
		println("Executed in " + (System.currentTimeMillis() - startTime) + " ms")
		result
	}

	override def isCorrect(result: String): Boolean = {
		readTextFileIntoString("src\\main\\resources\\formattedresults\\q_1_3_result.txt").equals(result)
	}


	override def toComparableString(results: AggregationResult): String = {
		results.getResults.get(0).get("total_revenue").toString
	}

	/**
	 * This method is used to build a date range String by providing a year and a weeknumber
	 * In the ssb dataset, the d_weeknuminyear field is not defined by actual weeks that start by a specific day
	 * Week 1 is always January 1th to January 6th, no matter what weekdays these dates are
	 *
	 * @param year
	 * @param weekNumber
	 * @return a formatted String of a date Range like this: [yyyyMMdd yyyyMMdd]
	 */
	private def getDateRangePerYearAndWeeknumber(year: Int, weekNumber: Int): String = {
		val lastDayNumber = weekNumber * 7 - 1
		val lastDay = LocalDate.ofYearDay(year, lastDayNumber)
		val firstDay = lastDay.minusDays(6)
		val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
		"[" + firstDay.format(dateFormatter) + " " + lastDay.format(dateFormatter) + "]"
	}
}
