package de.aljoshavieth.redisolapclient
package clientapproach.q1_2

import clientapproach.RedisQuery
import clientapproach.q1_1.Q1_1_client_c.queryDocuments

import redis.clients.jedis.search.{Document, Query}
import redis.clients.jedis.{JedisPooled, Pipeline}

object Q1_2_client_c extends RedisQuery {
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
			val dateRange = "@lo_orderdate:[" + doc.getString("d_datekey") + " " + doc.getString("d_datekey") + "]"
			List(dateRange) // return a List with the dateRange string
		}

		//println(d_datekeys)

		val queryString = d_datekeys.mkString(" | ")
		//println(queryString)
		val query = new Query(queryString)
		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 4, 6),
			new Query.NumericFilter("lo_quantity", 26, 35) 
		)
		val relevantLineOrderDocuments = queryDocuments(jedisPooled, "lineorder-index", query = query, filters = lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))
		//println("relevant lineorder documents: " + relevantLineOrderDocuments.length)
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX
		println("Revenue: " + revenue)
	}
}
