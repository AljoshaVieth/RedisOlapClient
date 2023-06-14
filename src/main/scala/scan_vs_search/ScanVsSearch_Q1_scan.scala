package de.aljoshavieth.redisolapclient
package scan_vs_search

import clientapproach.RedisQuery
import clientapproach.q1_1.Q1_1_client_a.{execute, queryDocuments}

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import redis.clients.jedis.search.{Document, Query}
import redis.clients.jedis.{Jedis, JedisPooled, Pipeline}

import scala.jdk.CollectionConverters.*


/**
 * In this object, an alternative approach to the usage of redisearch is tested.
 * Instead of indexing the lo_discount field using redisearch, a structure is created where the discount is part of the key.
 * To keep the example simpler, the lo_quantity, which is actually also a filter criterion for the lineorder documents, is ignored.
 */
object ScanVsSearch_Q1_scan {
	/**
	 * Original Q1.1 in SQL:
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_year = 1993
	 * and lo_discount between 1 and 3
	 * and lo_quantity < 25; <-- ignored in this case
	 */

	def execute(jedisPooled: JedisPooled, jedisDb1: Jedis) = { // Jedis for db1 instead of JedisPooled is used because JedisPooled can not switch the db
		val lowerBound: Int = 1
		val uppderBound: Int = 3
		jedisDb1.select(1)
		jedisDb1.getClient.setTimeoutInfinite()

		val pipeline: Pipeline = jedisDb1.pipelined()
		val cursor: String = ScanParams.SCAN_POINTER_START
		val patternStart: String = "lineorder:discount:"
		val scanParams: ScanParams = new ScanParams().count(Integer.MAX_VALUE).`match`(patternStart + 1 + ":*")
		val scanResult: ScanResult[String] = jedisDb1.scan(cursor, scanParams)
		println("size: " + scanResult.getResult.size())
		//pipeline.hmget(scanResult.getResult.get(0), "lo_orderdate", "lo_extendedprice", "lo_discount")
		//pipeline.hmget(scanResult.getResult.get(1), "lo_orderdate", "lo_extendedprice", "lo_discount")
		scanResult.getResult.forEach(key => pipeline.hmget(key, "lo_orderdate", "lo_extendedprice", "lo_discount"))
		/*
		for(i <- lowerBound to uppderBound) {
			val scanParams: ScanParams = new ScanParams().count(Integer.MAX_VALUE).`match`(patternStart + i + ":*")
					val scanResult: ScanResult[String] = jedisDb1.scan(cursor, scanParams)
			if(!scanResult.isCompleteIteration){
				throw Exception("Error, could not load all scan results in one go...") //TODO handle better
			}
			scanResult.getResult.forEach(key => pipeline.hmget(key, "lo_orderdate", "lo_extendedprice", "lo_discount"))
		}
		*/

		val lineorderDocuments = pipeline.syncAndReturnAll().asScala.toList
		println(lineorderDocuments)


		/*
		val dateFilters: List[Query.Filter] = List(new Query.NumericFilter("d_year", 1993, 1993))
		val query: Query = new Query()
		query
			.limit(0, Integer.MAX_VALUE) // Set the limit of results as high as possible
			.returnFields("d_datekey") // Define which fields should be included in the Document objects
			.timeout(Integer.MAX_VALUE) // Make sure to enable as much time as possible to the Query so it can get as much results as possible TODO: can maybe disabled with setting it to 0
		// Add filters
		dateFilters.foreach(filter => {
			query.addFilter(filter)
		})
		val dateDocuments: List[Document] = jedisPooled.ftSearch("date-index", query).getDocuments.asScala.toList
*/
	}

}
