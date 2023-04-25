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

	private val jedisURI = new URI("redis://localhost:6379")
	private val jedisPooled: JedisPooled = new JedisPooled(jedisURI, Integer.MAX_VALUE)
	private val jedisPipeline: Pipeline = jedisPooled.pipelined()


	def main(args: Array[String]): Unit = {
		jedisPooled.sendCommand(SearchCommand.CONFIG, "SET", "MAXSEARCHRESULTS", "-1")

		println("Running Q1.1 ...")
		println("Executed in: " + calculateExecutionTime(runQ1_1) + "ns")

		println("Running Q1.2 ...")
		println("Executed in: " + calculateExecutionTime(runQ1_2) + "ns")

		println("Running Q1.3 ...")
		println("Executed in: " + calculateExecutionTime(runQ1_3) + "ns")

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
		val dateFilters: List[Query.Filter] = List(new Query.NumericFilter("d_year", 1993, 1993))
		val dateDocuments: List[Document] = queryDocuments("date-index", dateFilters, List("d_datekey"))

		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 1, 3),
			new Query.NumericFilter("lo_quantity", 0, 24) // This is not quite correct, since it is assumed that quantity always >= 0
		)
		val lineorderDocuments = queryDocuments("lineorder-index", lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))

		val relevantLineOrderDocuments = filterDocuments(lineorderDocuments, "lo_orderdate", dateDocuments, "d_datekey")
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
		val dateFilters = List(
			new Query.NumericFilter("d_yearmonthnum", 199401, 199401))
		val dateDocuments = queryDocuments("date-index", dateFilters, List("d_datekey"))

		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 4, 6),
			new Query.NumericFilter("lo_quantity", 26, 35)
		)
		val lineorderDocuments = queryDocuments("lineorder-index", lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))

		val relevantLineOrderDocuments = filterDocuments(lineorderDocuments, "lo_orderdate", dateDocuments, "d_datekey")
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX

		println("Revenue: " + revenue)
	}

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
	private def runQ1_3(): Unit = {
		val dateFilters = List(
			new Query.NumericFilter("d_weeknuminyear", 6, 6),
			new Query.NumericFilter("d_year", 1994, 1994))
		val dateDocuments = queryDocuments("date-index", dateFilters, List("d_datekey"))

		val lineorderFilters = List(
			new Query.NumericFilter("lo_discount", 5, 7),
			new Query.NumericFilter("lo_quantity", 26, 35)
		)
		val lineorderDocuments = queryDocuments("lineorder-index", lineorderFilters, List("lo_orderdate", "lo_extendedprice", "lo_discount"))

		val relevantLineOrderDocuments = filterDocuments(lineorderDocuments, "lo_orderdate", dateDocuments, "d_datekey")
		val revenue = relevantLineOrderDocuments.map(doc => doc.getString("lo_extendedprice").toLong * doc.getString("lo_discount").toLong).sum // The usage of Long is crucial, since the result > Integer MAX

		println("Revenue: " + revenue)
	}


	/**
	 * This method generalises running queries against Redis to retrieve a list of documents
	 *
	 * @param indexName    The index that should be used
	 * @param filters      A list of Query.Filter objects to filter the results
	 * @param returnFields The fields that should be returned. This can be used to ignore irrelevant fields.
	 * @return A List[Document] with all relevant Documents found by the query
	 */
	private def queryDocuments(indexName: String, filters: List[Query.Filter], returnFields: List[String]): List[Document] = {
		val query = new Query()
			.limit(0, Integer.MAX_VALUE) // Set the limit of results as high as possible
			.returnFields(returnFields: _*) // Define which fields should be included in the Document objects
			.timeout(Integer.MAX_VALUE) // Make sure to enable as much time as possible to the Query so it can get as much results as possible

		// Add filters
		filters.foreach(filter => {
			query.addFilter(filter)
		})
		// Execute query and convert result to a scala List[Document]
		jedisPooled.ftSearch(indexName, query).getDocuments.asScala.toList
	}

	/**
	 * This method can be used to filter a List[Document] based on another List[Document]
	 *
	 * @param documentsToFilter     The List[Document] that should be filtered
	 * @param filterField1          The field of documentsToFilter based on which the comparison with the other list, documentsToFilterWith, should be done
	 * @param documentsToFilterWith The List[Documents] which is used to filter documentsToFilter
	 * @param filterField2          The field of documentsToFilterWith based on which the comparison with the other list, documentsToFilter, should be done
	 * @return A List[Document] that contains all Documents of documentsToFilter that match the filter criteria
	 */
	private def filterDocuments(documentsToFilter: List[Document], filterField1: String, documentsToFilterWith: List[Document], filterField2: String): List[Document] = {
		val filterValues = documentsToFilterWith.map(_.getString(filterField2)).toSet // Converting to set to enable faster comparison due to constant-time lookups
		documentsToFilter.filter(doc => filterValues.contains(doc.getString(filterField1)))
	}


	/**
	 * This function can be used to execute any other function while measuring the execution time
	 *
	 * @param f The function to be executed
	 * @return The execution time of f in nanoseconds
	 */
	private def calculateExecutionTime(f: () => Unit): Long = {
		val startTime = System.nanoTime
		f()
		System.nanoTime() - startTime
	}

}