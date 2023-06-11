package de.aljoshavieth.redisolapclient
package clientapproach.q1_2

import clientapproach.RedisQuery

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.search.Query

object Q1_2_client_a extends RedisQuery {
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
		val dateFilters = List(new Query.NumericFilter("d_yearmonthnum", 199401, 199401))
		val dateDocuments = queryDocuments(jedisPooled, "date-index", filters = dateFilters, List("d_datekey"))

		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 4, 6),
			new Query.NumericFilter("lo_quantity", 26, 35)
		)
		val lineorderDocuments = queryDocuments(jedisPooled, "lineorder-index", filters = lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))

		val relevantLineOrderDocuments = filterDocuments(lineorderDocuments, "lo_orderdate", dateDocuments, "d_datekey")
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX

		println("Revenue: " + revenue)
	}
}
