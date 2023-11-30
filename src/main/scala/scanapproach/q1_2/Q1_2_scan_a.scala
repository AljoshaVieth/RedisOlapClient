package de.aljoshavieth.redisolapclient
package scanapproach.q1_2

import scanapproach.RedisScanQuery

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import redis.clients.jedis.{Jedis, Pipeline}

import java.util
import scala.jdk.CollectionConverters.*

/**
 * This object tests an alternative approach.
 * It operates without RediSearch.
 * For this, the data must be pre-calculated accordingly.
 * This approach has proven impractical and was not pursued further.
 */
object Q1_2_scan_a extends RedisScanQuery {
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

	override def execute(jedis: Jedis): Unit = {
		val pipeline: Pipeline = jedis.pipelined()
		val pattern: String = "lineorder:orderdate:199401*"
		val scanParams: ScanParams = new ScanParams().count(Integer.MAX_VALUE).`match`(pattern)
		val startTime = System.currentTimeMillis()
		val resultingKeys: List[String] = scanForKeys(jedis, ScanParams.SCAN_POINTER_START, scanParams)
		println("Scan finished in " + (System.currentTimeMillis() - startTime) + "ms")

		resultingKeys.foreach(pipeline.hmget(_, "lo_extendedprice", "lo_discount", "lo_quantity"))
		val result = pipeline.syncAndReturnAll()

		val castedResult: List[List[Long]] =
			result.asScala.toList.map { innerList =>
				innerList.asInstanceOf[util.ArrayList[String]].asScala.toList.map(_.toLong)
			}
		val filteredResult = castedResult.filter(record => (record(1) >= 4) && (record(1) <= 6) && (record(2) >= 26) && (record(2) <= 35))
		val revenue: Long = filteredResult.map(lineoderDate => lineoderDate.head * lineoderDate(1)).sum
		println("Revenue: " + revenue)
	}


}
