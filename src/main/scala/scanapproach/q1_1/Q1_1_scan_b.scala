package de.aljoshavieth.redisolapclient
package scanapproach.q1_1

import scanapproach.RedisScanQuery

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import redis.clients.jedis.{Jedis, Pipeline}

import java.util
import scala.jdk.CollectionConverters.*

object Q1_1_scan_b extends RedisScanQuery {
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

	override def execute(jedis: Jedis): Unit = {
		jedis.select(2)
		val pipeline: Pipeline = jedis.pipelined()
		val pattern: String = "dateLineorderKeyIndex:1993*"
		val scanParams: ScanParams = new ScanParams().count(Integer.MAX_VALUE).`match`(pattern)
		val startTime = System.currentTimeMillis()
		val resultingKeys: List[String] = scanForKeys(jedis, ScanParams.SCAN_POINTER_START, scanParams)
		println("ResultingKeys: " + resultingKeys)
		println("Scan finished in " + (System.currentTimeMillis() - startTime) + "ms")
		resultingKeys.foreach(pipeline.lrange(_, 0, -1))
		val result = pipeline.syncAndReturnAll()
		//val cResult : List[String] = result.asScala.map(_.asInstanceOf[String]).toList
		//println(cResult.take(5))
		//println(result)
		//println(result.getClass)

		val castedResult: List[List[String]] = result.asScala.toList.map { innerList =>
			innerList.asInstanceOf[util.ArrayList[String]].asScala.toList.map(_.toString)
		}
		val lineorderKeys = castedResult.flatten.map(key => "lineorder:" + key)
		lineorderKeys.foreach(pipeline.hmget(_, "lo_extendedprice", "lo_discount", "lo_quantity"))
		val newResult = pipeline.syncAndReturnAll()

		val newCastedResult: List[List[Long]] =
			newResult.asScala.toList.map { innerList =>
				innerList.asInstanceOf[util.ArrayList[String]].asScala.toList.map(_.toLong)
			}
		val filteredResult = castedResult.filter(record => (record(1) >= 1) && (record(1) <= 3) && (record(2) < 25))
		val revenue: Long = filteredResult.map(lineoderDate => lineoderDate.head * lineoderDate(1)).sum
		println("Revenue: " + revenue)

		/*
	val filteredResult = castedResult.filter(record => (record(1) >= 1) && (record(1) <= 3) && (record(2) < 25))
	val revenue: Long = filteredResult.map(lineoderDate => lineoderDate.head * lineoderDate(1)).sum
	println("Revenue: " + revenue)
		*/

	}

}
