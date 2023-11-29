package de.aljoshavieth.redisolapclient
package clientapproach.q2_1

import clientapproach.{GroupByHelper, RedisQuery}

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
 * and p_category = 'MFGR#12'
 * and s_region = 'AMERICA'
 * group by d_year, p_brand1
 * order by d_year, p_brand1;
 */
object Q2_1_client_b extends RedisQuery {
	override def execute(jedisPooled: JedisPooled): Unit = {
		val partQuery: Query = new Query("@p_category:{MFGR\\#12}")
		val partDocuments: List[Document] = queryDocuments(jedisPooled, "part-index", partQuery, returnFields = List("p_brand1", "p_partkey"))

		val validPartKeys = partDocuments.flatMap { doc =>
			val validSuppKeys = "@lo_partkey:[" + doc.getString("p_partkey") + " " + doc.getString("p_partkey") + "]"
			List(validSuppKeys)
		}

		val validPartKeysQuery = validPartKeys.mkString(" | ")

		val supplierQuery: Query = new Query("@s_region:{AMERICA}")
		val supplierDocuments: List[Document] = queryDocuments(jedisPooled, "supplier-index", supplierQuery, returnFields = List("s_suppkey"))

		val validSuppkeys = supplierDocuments.flatMap { doc =>
			val validSuppKeyRanges = "@lo_suppkey:[" + doc.getString("s_suppkey") + " " + doc.getString("s_suppkey") + "]"
			List(validSuppKeyRanges)
		}
		val validSuppkeysQuery = validSuppkeys.mkString(" | ")


		val dateDocuments: List[Document] = queryDocuments(jedisPooled, "date-index", returnFields = List("d_year", "d_datekey"))

		val validDatekeys = dateDocuments.flatMap { doc =>
			val validDateRanges = "@lo_orderdate:[" + doc.getString("d_datekey") + " " + doc.getString("d_datekey") + "]"
			List(validDateRanges)
		}

		val validDateKeysQuery = validDatekeys.mkString(" | ")

		val completeQuery = validPartKeysQuery + " " +  validSuppkeysQuery + " " + validDateKeysQuery

		val relevantLineorderDocuments = queryDocuments(jedisPooled, "lineorder-index", query = Query(completeQuery), returnFields = List("lo_revenue", "lo_orderdate", "lo_partkey", "lo_suppkey"))


		// Replace lo_partkey and lo_orderdate with proper p_brand1 and d_year
		val updatedLineorderDocuments = GroupByHelper.exchangeDocumentProperties(partDocuments, dateDocuments, relevantLineorderDocuments)

		val grouped: Map[(String, String), List[Document]] = updatedLineorderDocuments.groupBy(doc => (doc.getString("d_year"), doc.getString("p_brand1")))


		val result: List[((String, String), Long)] = grouped.view.mapValues(docs => docs.map(_.getString("lo_revenue").toLong).sum).toList.sortBy(_._1)
		println("    sum   |    d_year  |    p_brand1    ")
		result.foreach(x => println(x._2 + " |    " + x._1._1 + "    |    " + x._1._2))
		println(result.size + " results")
	}
}
