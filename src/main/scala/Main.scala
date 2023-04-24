package de.aljoshavieth.redisolapclient

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.search.SearchProtocol.SearchCommand
import redis.clients.jedis.search.{Document, Query}
import redis.clients.jedis.{Jedis, JedisPool, JedisPooled, Pipeline}

import java.io.{File, PrintWriter}
import java.net.URI
import java.util
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*


object Main {

	private val jedisURI = new URI("redis://localhost:6379"); // replace with your Redis server URI

	private val jedisPooled: JedisPooled = new JedisPooled(jedisURI, Integer.MAX_VALUE)

	private val jedisPipeline: Pipeline = jedisPooled.pipelined()


	def main(args: Array[String]): Unit = {
		jedisPooled.sendCommand(SearchCommand.CONFIG, "SET", "MAXSEARCHRESULTS", "-1")

		println("Running Q1.1 ...")
		val startTimeQ1_1 = System.nanoTime
		runQ1_1()
		println("\nAll data extracted in " + (System.nanoTime() - startTimeQ1_1) + " nanoseconds")

		println("Running Q1.2 ...")
		val startTimeQ1_2 = System.nanoTime
		runQ1_2()
		println("\nAll data extracted in " + (System.nanoTime() - startTimeQ1_2) + " nanoseconds")
		jedisPipeline.close()
		jedisPooled.close()
	}

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

	/**
	 * This method performs the Query 1.1 of the Star Schema Benchmark on a Redis database
	 * First, it queries all documents of the date "table" (hashes in Redis) where the field "d_year" equals 1993
	 * Second, it queries all documents of the lineorder "table"
	 * 	where the value of the field "lo_discount" is between 1 and 3
	 * 	and the "lo_quantity" is between 0 and 24
	 *
	 * The method than filters the received lineorder documents based on the occurence of their "lo_orderkey" in the
	 * 	documents queried earlier from the date table.
	 *
	 * Finally, it sums up all (lo_extendedprice*lo_discount) of the lineorder documents and prints the result.
	 *
	 */
	private def runQ1_1(): Unit = {
		// First, get all keys of hashes where d_year = 1993
		val getAllMatchingDatesQuery = new Query()
			.addFilter(new Query.NumericFilter("d_year", 1993, 1993)) // Filter using the yeas
			.limit(0, Integer.MAX_VALUE) // Get as many results as possible with the Integer max
			.returnFields("d_datekey") // Only return the d_datekey to save space
			.timeout(Integer.MAX_VALUE) // The timeout parameter is very important. If the timout is reached, it takes the ones it already has, which can result in different results running the same query

		// Search for documents matching the query in the "date-index" index
		val dateSearchResult = jedisPooled.ftSearch("date-index", getAllMatchingDatesQuery)
		val relevantDates: List[String] = dateSearchResult.getDocuments.asScala.toList.map(_.getString("d_datekey"))

		//println("Found " + relevantDates.length + " relevant dates.")


		// Second, get all keys and all values of the fields lo_orderdate, lo_extendedprice and lo_discount from the lineorder hashes
		val getAllLineorderDatesQuery = new Query()
			.addFilter(new Query.NumericFilter("lo_discount", 1, 3))
			.addFilter(new Query.NumericFilter("lo_quantity", 0, 24)) //TODO: this is not quite correct, since it is assumed that quantitiy always >= 0
			.limit(0, Integer.MAX_VALUE)
			.returnFields("lo_orderdate", "lo_extendedprice", "lo_discount")
			.timeout(Integer.MAX_VALUE) // The timeout parameter is very important. If the timout is reached, it takes the ones it already has, which can result in different results running the same query

		val lineorderSearchResults = jedisPooled.ftSearch("lineorder-index", getAllLineorderDatesQuery)
		val lineorderDocuments: List[Document] = lineorderSearchResults.getDocuments.asScala.toList

		//println("Found " + lineorderDocuments.length + " matching lineorder documents.")

		val relevantLineOrderDocuments = lineorderDocuments.filter(doc => relevantDates.contains(doc.getString("lo_orderdate")))
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX

		println("Revenue: " + revenue)
	}

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
	private def runQ1_2(): Unit = {
		// First, get all keys of hashes where d_yearmonthnum has the value 199401
		val getAllMatchingDatesQuery = new Query()
			.addFilter(new Query.NumericFilter("d_yearmonthnum", 199401, 199401)) // Filter using the yeas
			.limit(0, Integer.MAX_VALUE) // Get as many results as possible with the Integer max
			.returnFields("d_datekey") // Only return the d_datekey to save space
			.timeout(Integer.MAX_VALUE) // The timeout parameter is very important. If the timout is reached, it takes the ones it already has, which can result in different results running the same query

		// Search for documents matching the query in the "date-index" index
		val dateSearchResult = jedisPooled.ftSearch("date-index", getAllMatchingDatesQuery)
		val relevantDates: List[String] = dateSearchResult.getDocuments.asScala.toList.map(_.getString("d_datekey"))

		//println("Found " + relevantDates.length + " relevant dates.")


		// Second, get all keys and all values of the fields lo_orderdate, lo_extendedprice and lo_discount from the lineorder hashes
		val getAllLineorderDatesQuery = new Query()
			.addFilter(new Query.NumericFilter("lo_discount", 4, 6))
			.addFilter(new Query.NumericFilter("lo_quantity", 26, 35))
			.limit(0, Integer.MAX_VALUE)
			.returnFields("lo_orderdate", "lo_extendedprice", "lo_discount")
			.timeout(Integer.MAX_VALUE) // The timeout parameter is very important. If the timout is reached, it takes the ones it already has, which can result in different results running the same query

		val lineorderSearchResults = jedisPooled.ftSearch("lineorder-index", getAllLineorderDatesQuery)
		val lineorderDocuments: List[Document] = lineorderSearchResults.getDocuments.asScala.toList

		//println("Found " + lineorderDocuments.length + " matching lineorder documents.")

		val relevantLineOrderDocuments = lineorderDocuments.filter(doc => relevantDates.contains(doc.getString("lo_orderdate")))
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX

		println("Revenue: " + revenue)
	}
}