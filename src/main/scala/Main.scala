package de.aljoshavieth.redisolapclient

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.search.SearchProtocol.SearchCommand
import redis.clients.jedis.search.{Document, Query}
import redis.clients.jedis.{Jedis, JedisPool, JedisPooled, Pipeline}

import java.net.URI
import java.util
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*


object Main {

	val jedisURI = new URI("redis://localhost:6379"); // replace with your Redis server URI

	val jedisPooled: JedisPooled = new JedisPooled(jedisURI, Integer.MAX_VALUE)

	val jedisPipeline = jedisPooled.pipelined()


	def main(args: Array[String]): Unit = {
		val startTime = System.nanoTime
		println("Running query data...")
		jedisPooled.sendCommand(SearchCommand.CONFIG, "SET", "MAXSEARCHRESULTS", "-1")
		runQ1_1()
		println("\nAll data extracted in " + (System.nanoTime() - startTime) + " nanoseconds")
		jedisPipeline.close()
		jedisPooled.close()
	}

	def runQ1_1(): Unit = {
		/**
		 * First, get all keys of hashes where d_year = 1993
		 */
		val getAllMatchingDatesQuery = new Query()
			.addFilter(new Query.NumericFilter("d_year", 1993, 1993)) // Filter using the yeas
			.limit(0, Integer.MAX_VALUE) // Get as many results as possible with the Integer max
			.returnFields("d_datekey") // Only return the d_datekey to save space
			.timeout(Integer.MAX_VALUE) // The timeout parameter is very important. If the timout is reached, it takes the ones it already has, which can result in different results running the same query

		println(getAllMatchingDatesQuery)
		// Search for documents matching the query in the "date-index" index
		val dateSearchResult = jedisPooled.ftSearch("date-index", getAllMatchingDatesQuery)
		println(dateSearchResult.getDocuments)
		// Get the list of documents matching the query
		val dateDocuments: List[Document] = dateSearchResult.getDocuments.asScala.toList // Converting Java types to Scala
		println("Found " + dateDocuments.length + " matching documents.")
		// Store all valid keys (id) in a Map where the d_datekey is the key
		val matchingDateIds: Map[String, String] = dateDocuments.map(doc => (doc.getString("d_datekey"), doc.getId)).toMap
		//matchingDateIds.foreach(println)
		println(jedisPooled.ftInfo("lineorder-index"))

		/**
		 * Second, get all keys and all values of the fields lo_orderdate from the lineorder hashes
		 */
		val getAllLineorderDatesQuery = new Query()
			.addFilter(new Query.NumericFilter("lo_discount", 1, 3))
			.addFilter(new Query.NumericFilter("lo_quantity", 0, 24)) //TODO: this is not quite correct, since it is assumed that quantitiy always >= 0
			.limit(0, Integer.MAX_VALUE)
			.returnFields("lo_orderdate")
			.timeout(Integer.MAX_VALUE) // The timeout parameter is very important. If the timout is reached, it takes the ones it already has, which can result in different results running the same query


		val lineorderSearchResults = jedisPooled.ftSearch("lineorder-index", getAllLineorderDatesQuery)
		val lineorderDocuments: List[Document] = lineorderSearchResults.getDocuments.asScala.toList
		println("Found " + lineorderDocuments.length + " matching lineorder documents.")
		val lineorderDateMap = lineorderDocuments.map(doc => (doc.getString("lo_orderdate"), doc.getId))
		println(lineorderDateMap.take(5))
		println(lineorderDateMap.length)
		/*
		val matchingLineOrderIds: List[String] = matchingDateIds.keys.flatMap(k => lineorderDateMap.get(k)).toList

		matchingLineOrderIds.foreach(key => jedisPipeline.hgetAll(key))
		val result = jedisPipeline.syncAndReturnAll().asScala.toList
		//val documents: List[Document] = result.map(_.asInstanceOf[Document])
		println("Results: " + result.length)
		//val revenue: util.HashMap[String, Integer] = new util.HashMap()

		val revenues = ListBuffer.empty[Int];
		result.foreach(x => {
			val g: util.HashMap[String, Object] = x.asInstanceOf[java.util.HashMap[String, Object]]
			revenues +=(g.get("lo_extendedprice").toString.toInt*g.get("lo_discount").toString.toInt)
			//revenue.put("lineorder:"+g.get("lo_orderkey") + ":" + g.get("lo_linenumber"), (g.get("lo_extendedprice").toString.toInt*g.get("lo_discount").toString.toInt))
		})
		println("Revenue: " + revenues.sum)


		//revenue.forEach((k, v) => println("revenue:\n" + k + ": " + v + "\n"))


		 */
	}
}