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
import java.time.format.DateTimeFormatter
import java.time.temporal.{TemporalAdjusters, WeekFields}
import java.time.{DayOfWeek, LocalDate}
import java.util
import java.util.Locale
import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*


object Q1_3_client_e extends RedisQuery {

	/**
	 * Original Q1.1 in SQL:
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_year = 1993
	 * and lo_discount between 1 and 3
	 * and lo_quantity < 25;
	 */


	override def execute(jedisPooled: JedisPooled): Unit = {
		val reducer: Reducer = Reducers.sum("revenue").as("total_revenue")
		val aggregation = new AggregationBuilder("@lo_discount:[5 7] @lo_quantity:[26 35] @lo_orderdate:" + getDateRangePerYearAndWeeknumber(1994, 6))
			.load("@lo_discount", "@lo_extendedprice")
			.apply("@lo_discount * @lo_extendedprice", "revenue")
			.groupBy(List.empty[String].asJavaCollection, List(reducer).asJavaCollection)
			.limit(0, Integer.MAX_VALUE) // Optional, set your limit

		val result: AggregationResult = jedisPooled.ftAggregate("lineorder-index", aggregation)
		println("Revenue: " + result.getResults.get(0).get("total_revenue"))

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
