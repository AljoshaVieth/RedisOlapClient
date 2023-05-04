package de.aljoshavieth.redisolapclient
package ssbqueries

import ssbqueries.Q1_1.queryDocuments

import redis.clients.jedis.search.{Document, Query, SearchResult}
import redis.clients.jedis.{JedisPooled, Pipeline}

import scala.jdk.CollectionConverters.*


object Q1_1_d extends RedisQuery {

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
		val dateFilters: List[Query.Filter] = List(new Query.NumericFilter("d_year", 1993, 1993))
		val dateDocuments: List[Document] = queryDocuments(jedisPooled, "date-index", filters = dateFilters, List("d_datekey"))
		println("found date obejcts: " + dateDocuments.length)

		val pipeline: Pipeline = jedisPooled.pipelined()


		val d_datekeys = dateDocuments.flatMap { doc =>
			val dateRange = "@lo_orderdate:[" + doc.getString("d_datekey") + " " + doc.getString("d_datekey") + "]"
			List(dateRange) // return a List with the dateRange string
		}

		println(d_datekeys) // output: List("@lo_orderdate:[20220502 20220502]", "@lo_orderdate:[20220503 20220503]", ...)

		val queryString = d_datekeys.mkString(" | ")
		println(queryString)
		val query = new Query(queryString)
		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 1, 3),
			new Query.NumericFilter("lo_quantity", 0, 24) // This is not quite correct, since it is assumed that quantity always >= 0
		)
		val relevantLineOrderDocuments = queryDocuments(jedisPooled, "lineorder-index", query = query, filters = lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))
		println("relevant lineorder documents: " + relevantLineOrderDocuments.length)
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX
		println("Revenue: " + revenue)
		
	}
}
