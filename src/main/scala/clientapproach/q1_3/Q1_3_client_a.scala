package de.aljoshavieth.redisolapclient
package clientapproach.q1_3

import clientapproach.RedisQuery

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.search.Query

object Q1_3_client_a extends RedisQuery {
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
		val dateFilters = List(
			new Query.NumericFilter("d_weeknuminyear", 6, 6),
			new Query.NumericFilter("d_year", 1994, 1994))
		val dateDocuments = queryDocuments(jedisPooled, "date-index", filters = dateFilters, List("d_datekey"))

		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 5, 7),
			new Query.NumericFilter("lo_quantity", 26, 35)
		)
		val lineorderDocuments = queryDocuments(jedisPooled, "lineorder-index", filters = lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))

		val relevantLineOrderDocuments = filterDocuments(lineorderDocuments, "lo_orderdate", dateDocuments, "d_datekey")
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX

		println("Revenue: " + revenue)
	}
}
