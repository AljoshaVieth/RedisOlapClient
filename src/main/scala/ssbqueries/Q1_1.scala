package de.aljoshavieth.redisolapclient
package ssbqueries

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.search.{Document, Query}

/**
 * This class is used to perform the Query 1.1 of the Star Schema Benchmark on a Redis database
 * First, it queries all documents of the date "table" (hashes in Redis) where the field "d_year" equals 1993
 * Second, it queries all documents of the lineorder "table"
 * where the value of the field "lo_discount" is between 1 and 3
 * and the "lo_quantity" is between 0 and 24
 *
 * The method than filters the received lineorder documents based on the occurence of their "lo_orderkey" in the
 * documents queried earlier from the date table.
 *
 * Finally, it sums up all (lo_extendedprice*lo_discount) of the lineorder documents and prints the result.
 *
 */
object Q1_1 extends RedisQuery {

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

		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 1, 3),
			new Query.NumericFilter("lo_quantity", 0, 24) // This is not quite correct, since it is assumed that quantity always >= 0
		)
		val lineorderDocuments = queryDocuments(jedisPooled, "lineorder-index", filters = lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))

		val relevantLineOrderDocuments = filterDocuments(lineorderDocuments, "lo_orderdate", dateDocuments, "d_datekey")
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX
		println("Revenue: " + revenue)
	}
}
