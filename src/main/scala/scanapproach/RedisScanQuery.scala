package de.aljoshavieth.redisolapclient
package scanapproach

import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import scala.jdk.CollectionConverters.*

/**
 * This class acts as a base class for  an alternative approach.
 * It operates without RediSearch.
 * For this, the data must be pre-calculated accordingly.
 * This approach has proven impractical and was not pursued further.
 */
abstract class RedisScanQuery {
	def execute(jedis: Jedis): Unit

	/**
	 * The Redis Scan command does not ensure that all possible results are returned when it is run once.
	 * The command returns a cursor that can be used to continue from the position just reached when executed again.
	 * This recursive method ensures that the scan continues until all results have been found.
	 *
	 * @param jedis              A Jedis instance to communicate with the database
	 * @param cursor             The current cursor where to scan from
	 * @param scanParams         The details of what should be scanned
	 * @param currentScanResults A list of current results, used for recursion
	 * @return A List[String] with keys matching the scan pattern
	 */
	protected def scanForKeys(jedis: Jedis, cursor: String, scanParams: ScanParams, currentScanResults: List[String] = List.empty[String]): List[String] = {
		val scanResult: ScanResult[String] = jedis.scan(cursor, scanParams)
		val scanResults = currentScanResults ++ scanResult.getResult.asScala
		if (!scanResult.isCompleteIteration) {
			println("Could not get all scan results in one go. Scanning again starting from offset " + scanResult.getCursor)
			scanForKeys(jedis, scanResult.getCursor, scanParams, scanResults)
		} else {
			scanResults
		}
	}
}
