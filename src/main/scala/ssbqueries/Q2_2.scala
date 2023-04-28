package de.aljoshavieth.redisolapclient
package ssbqueries

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
 * and p_brand1 between 'MFGR#2221' and 'MFGR#2228'
 * and s_region = 'ASIA'
 * group by d_year, p_brand1
 * order by d_year, p_brand1;
 */
object Q2_2 extends RedisQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {

		//println("Querying part documents...")
		val partDocuments = queryDocuments(jedisPooled, "part-index", returnFields = List("p_brand1", "p_partkey"))
		//println(partDocuments.length)

		val relevantPartDocuments = partDocuments.filter(p => {
			(p.getString("p_brand1").compareTo("MFGR#2221") >= 0 && p.getString("p_brand1").compareTo("MFGR#2228") <= 0)
		})
		//println(relevantPartDocuments.length)


		val lineorderDocuments = queryDocuments(jedisPooled, "lineorder-index", returnFields = List("lo_revenue", "lo_orderdate", "lo_partkey", "lo_suppkey"))
		//println("Queried: " + lineorderDocuments.length + " lineoderDocuments");

		val dateDocuments = queryDocuments(jedisPooled, "date-index", returnFields = List("d_year", "d_datekey"))


		val supplierQuery: Query = new Query("@s_region:ASIA")
		val supplierDocuments = queryDocuments(jedisPooled, "supplier-index", supplierQuery, returnFields = List("s_suppkey"))


		//println("Queried: " + supplierDocuments.length + " supplierDocuments");




		val relevantLineOrderDocuments = lineorderDocuments
			.pipe(filterDocuments(_, "lo_suppkey", supplierDocuments, "s_suppkey"))
			.pipe(filterAndJoinDocuments(_, "lo_partkey", relevantPartDocuments, "p_partkey", List("p_brand1")))
			.pipe(filterAndJoinDocuments(_, "lo_orderdate", dateDocuments, "d_datekey", List("d_year")))


		val grouped = relevantLineOrderDocuments.groupBy(doc => (doc.getString("d_year"), doc.getString("p_brand1")))


		val result: List[((String, String), Long)] = grouped.view.mapValues(docs => docs.map(_.getString("lo_revenue").toLong).sum).toList.sortBy(_._1)
		//println(result.head)
		//println(" ")
		println("    sum   |    d_year  |    p_brand1    ")

		result.foreach(x => println(x._2 + " |    " + x._1._1 + "    |    " + x._1._2))
		println(result.size + " results")


	}
}
