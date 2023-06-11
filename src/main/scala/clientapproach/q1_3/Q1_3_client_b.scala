package de.aljoshavieth.redisolapclient
package clientapproach.q1_3

import clientapproach.RedisQuery

import redis.clients.jedis.search.{Document, Query, SearchResult}
import redis.clients.jedis.{JedisPooled, Pipeline}

import scala.jdk.CollectionConverters.*


/**
 * In Difference to the other approcahes, this approach first fetches all relevant date documents and then
 * uses a pipeline to query every lineorder document in a seperate request
 */
object Q1_3_client_b extends RedisQuery {

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
		val dateFilters = List(
			new Query.NumericFilter("d_weeknuminyear", 6, 6),
			new Query.NumericFilter("d_year", 1994, 1994))
		val dateDocuments: List[Document] = queryDocuments(jedisPooled, "date-index", filters = dateFilters, List("d_datekey"))
		//println("found date obejcts: " + dateDocuments.length)

		val pipeline: Pipeline = jedisPooled.pipelined()
		val filters = List(
			new Query.NumericFilter("lo_discount", 5, 7),
			new Query.NumericFilter("lo_quantity", 26, 35)
		)
		val returnFields = List("lo_orderdate", "lo_extendedprice", "lo_discount")


		dateDocuments.foreach(dateDocument => {
			val d_datekey = dateDocument.getString("d_datekey").toInt
			val query = new Query()
			query
				.limit(0, Integer.MAX_VALUE) // Set the limit of results as high as possible
				.returnFields(returnFields: _*) // Define which fields should be included in the Document objects
				.timeout(Integer.MAX_VALUE) // Make sure to enable as much time as possible to the Query so it can get as much results as possible
			filters.foreach(query.addFilter(_))

			query.addFilter(new Query.NumericFilter("lo_orderdate", d_datekey, d_datekey))
			pipeline.ftSearch("lineorder-index", query)
		})

		val searchResults: List[SearchResult] = pipeline.syncAndReturnAll().asScala.toList.map(_.asInstanceOf[SearchResult])
		val relevantLineOrderDocuments: List[Document] = searchResults.flatMap(_.getDocuments.asScala)

		//println("relevant lineorder documents: " + relevantLineOrderDocuments.length)
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX
		println("Revenue: " + revenue)

	}
}
