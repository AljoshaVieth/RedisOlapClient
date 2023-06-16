package de.aljoshavieth.redisolapclient
package scanapproach.q1_3

import scanapproach.RedisScanQuery

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import redis.clients.jedis.{Jedis, Pipeline}

import java.util
import scala.jdk.CollectionConverters.*

object Q1_3_scan_a extends RedisScanQuery {
	/**
	 * Original Q1.3 Query in SQL
	 *
	 * select sum(lo_extendedprice*lo_discount) as revenue
	 * from lineorder, date
	 * where lo_orderdate = d_datekey
	 * and d_weeknuminyear = 6
	 * and d_year = 1994
	 * and lo_discount between 5 and 7
	 * and lo_quantity between 26 and 35;
	 */


	override def execute(jedis: Jedis): Unit = {
		// The weeknumber is not part of the orderdate, so it is not possible to match based on this
	}


}
