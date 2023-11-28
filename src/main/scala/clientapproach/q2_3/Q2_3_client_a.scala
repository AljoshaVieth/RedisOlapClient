package de.aljoshavieth.redisolapclient
package clientapproach.q2_3

import clientapproach.RedisQuery

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.search.{Document, Query}

import scala.util.chaining.scalaUtilChainingOps


/**
 * Original SQL Query:
 *
 * select sum(lo_revenue), d_year, p_brand1
 * from lineorder, date, part, supplier
 * where lo_orderdate = d_datekey
 * and lo_partkey = p_partkey
 * and lo_suppkey = s_suppkey
 * and p_brand1 = 'MFGR#2221'
 * and s_region = 'EUROPE'
 * group by d_year, p_brand1
 * order by d_year, p_brand1;
 *
 */
object Q2_3_client_a extends RedisQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		val partQuery: Query = new Query("@p_brand1:{MFGR\\#2221}")
		val partDocuments = queryDocuments(jedisPooled, "part-index", partQuery, returnFields = List("p_brand1", "p_partkey"))

		val supplierQuery: Query = new Query("@s_region:{EUROPE}")
		val supplierDocuments = queryDocuments(jedisPooled, "supplier-index", supplierQuery, returnFields = List("s_suppkey"))


		val dateDocuments: List[Document] = queryDocuments(jedisPooled, "date-index", returnFields = List("d_year", "d_datekey"))


		val lineorderDocuments = queryDocuments(jedisPooled, "lineorder-index", returnFields = List("lo_revenue", "lo_orderdate", "lo_partkey", "lo_suppkey"))

		println("numPartx: " + partDocuments.size)
		println("numSup: " + supplierDocuments.size)
		println("numLine: " + lineorderDocuments.size)

		val relevantLineOrderDocuments = lineorderDocuments
			.pipe(filterDocuments(_, "lo_suppkey", supplierDocuments, "s_suppkey"))
			.pipe(filterAndJoinDocuments(_, "lo_partkey", partDocuments, "p_partkey", List("p_brand1")))
			.pipe(filterAndJoinDocuments(_, "lo_orderdate", dateDocuments, "d_datekey", List("d_year")))

		val grouped: Map[(String, String), List[Document]] = relevantLineOrderDocuments.groupBy(doc => (doc.getString("d_year"), doc.getString("p_brand1")))


		val result: List[((String, String), Long)] = grouped.view.mapValues(docs => docs.map(_.getString("lo_revenue").toLong).sum).toList.sortBy(_._1)
		println("    sum   |    d_year  |    p_brand1    ")

		result.foreach(x => println(x._2 + " |    " + x._1._1 + "    |    " + x._1._2))
		println(result.size + " results")
	}
}
