package de.aljoshavieth.redisolapclient

import clientapproach.*
import clientapproach.q1_1.{Q1_1_client_a, Q1_1_client_b, Q1_1_client_c, Q1_1_client_d, Q1_1_client_d_alternative, Q1_1_client_e}
import clientapproach.q1_2.{Q1_2_client_a, Q1_2_client_b, Q1_2_client_c, Q1_2_client_d, Q1_2_client_d_alternative, Q1_2_client_e}
import clientapproach.q1_3.{Q1_3_client_a, Q1_3_client_b, Q1_3_client_c, Q1_3_client_d, Q1_3_client_d_alternative, Q1_3_client_e}
import clientapproach.q2_1.Q2_1_client_a
import clientapproach.q2_2.Q2_2_client_a
import serverapproach.LuaScriptLoader
import serverapproach.q1_1.{Q1_1_server_a, Q1_1_server_b, Q1_1_server_c, Q1_1_server_d}
import serverapproach.q1_2.{Q1_2_server_a, Q1_2_server_b, Q1_2_server_c, Q1_2_server_d}

import de.aljoshavieth.redisolapclient.scan_vs_search.ScanVsSearch_Q1_scan
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
		configureRedis()

		runClientApproachQueries()
		runServerApproachQueries()
		//runScanVsSearchQueries()

		jedisPipeline.close()
		jedisPooled.close()
	}

	private def runServerApproachQueries(): Unit = {
		println("\n----------------------------------------")
		println("Running server-based queries ...")
		println("........................................")



		// Q 1.1
		println("\n    Q1.1\n")
		println("Running Q1.1 server_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_a.execute(jedisPooled)) + "ms\n")

		println("Running Q1.1 server_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_b.execute(jedisPooled)) + "ms\n")

		println("Running Q1_1 server_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_c.execute(jedisPooled)) + "ms\n")

		println("Running Q1.1 server_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_d.execute(jedisPooled)) + "ms\n")

		// Q 1.2
		println("\n    Q1.2\n")
		println("Running Q1.2 server_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_a.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 server_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_b.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 server_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_c.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 server_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_d.execute(jedisPooled)) + "ms\n")


		//println("querySpecificDocuments: " + jedisPooled.fcall("querySpecificDocuments", List[String]().asJava, List[String]().asJava))
		//println(jedisPooled.fcall("queryDocuments", List[String]().asJava, List("date-index", "@d_year:[1993 1993]").asJava))
		//println(jedisPooled.fcall("queryFilterCriteria", List[String]().asJava, List("date-index", "@d_year:[1993 1993]", "d_datekey", "lo_orderdate").asJava)) //TODO this has to be wrapped
		//println("Executed in: " + calculateExecutionTime(println(jedisPooled.fcall("runQ1_1", List[String]().asJava, List("date-index", "@d_year:[1993 1993]", "d_datekey", "lo_orderdate").asJava))) + "ns")
		//println(jedisPooled.fcall("runQ1_1", List[String]().asJava, List("date-index", "@d_year:[1993 1993]", "d_datekey", "lo_orderdate").asJava)) //TODO this has to be wrapped

	}

	private def runClientApproachQueries(): Unit = {
		println("\n----------------------------------------")
		println("Running client-based queries ...")
		println("........................................")



		// Q 1.1
		println("\n    Q1.1\n")
		println("Running Q1.1 client_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_client_a.execute(jedisPooled)) + "ms\n")

		println("Running Q1.1 client_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_client_b.execute(jedisPooled)) + "ms\n")

		println("Running Q1.1 client_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_client_c.execute(jedisPooled)) + "ms\n")

		//println("Running Q1.1 client_d_alternative ...")
		//println("Executed in: " + calculateExecutionTime(Q1_1_client_d_alternative.execute(jedisPooled)) + "ms\n")

		println("Running Q1.1 client_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_client_d.execute(jedisPooled)) + "ms\n")

		println("Running Q1.1 client_e ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_client_e.execute(jedisPooled)) + "ms\n")


		// Q 1.2
		println("\n    Q1.2\n")
		println("Running Q1.2 client_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_client_a.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 client_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_client_b.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 client_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_client_c.execute(jedisPooled)) + "ms\n")

		//println("Running Q1.2 client_d_alternative ...")
		//println("Executed in: " + calculateExecutionTime(Q1_2_client_d_alternative.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 client_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_client_d.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 client_e ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_client_e.execute(jedisPooled)) + "ms\n")




		// Q 1.3
		println("\n    Q1.3\n")
		println("Running Q1.3 client_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_a.execute(jedisPooled)) + "ms\n")

		println("Running Q1.3 client_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_b.execute(jedisPooled)) + "ms\n")

		println("Running Q1.3 client_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_c.execute(jedisPooled)) + "ms\n")

		//println("Running Q1.3 client_d_alternative ...")
		//println("Executed in: " + calculateExecutionTime(Q1_3_client_d_alternative.execute(jedisPooled)) + "ms\n")

		println("Running Q1.3 client_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_d.execute(jedisPooled)) + "ms\n")

		println("Running Q1.3 client_e ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_e.execute(jedisPooled)) + "ms\n")




				// Q 2.1
				println("\n    Q2.1\n")
				println("Running Q2.1 ...")
				println("Executed in: " + calculateExecutionTime(Q2_1_client_a.execute(jedisPooled)) + "ms\n")

				/*
				println("\n----------------------------------------")
				println("Running Q2.1_c ...")
				println("Executed in: " + calculateExecutionTime(Q2_1_c.execute(jedisPooled)) + "ms")

				 */


				println("\n----------------------------------------")
				println("Running Q2.2 ...")
				println("Executed in: " + calculateExecutionTime(Q2_2_client_a.execute(jedisPooled)) + "ms\n")


	}

	private def runScanVsSearchQueries(): Unit = {
		val jedis = new Jedis("localhost", 6379)
		jedis.select(1) // Set the database index
		println("\n----------------------------------------")
		println("Running scan-vs-search queries ...")
		println("........................................")



		println("Running Q1_scan ...")
		println("Executed in: " + calculateExecutionTime(ScanVsSearch_Q1_scan.execute(jedisPooled, jedis)) + "ms")


	}

	private def configureRedis(): Unit = {
		println("Configuring Redis...")
		println(jedisPooled.functionLoadReplace(LuaScriptLoader.loadLuaScript("src/main/resources/olaplibrary.lua")))
		println("Set function")
		jedisPooled.sendCommand(SearchCommand.CONFIG, "SET", "MAXSEARCHRESULTS", "-1")
		jedisPooled.sendCommand(SearchCommand.CONFIG, "SET", "MAXAGGREGATERESULTS", "-1")
		jedisPooled.sendCommand(SearchCommand.CONFIG, "SET", "TIMEOUT", "0")
	}


	/**
	 * This function can be used to execute any other function while measuring the execution time
	 *
	 * @param f The function to be executed
	 * @return The execution time of f in nanoseconds
	 */
	private def calculateExecutionTime(f: => Unit): Long = {
		val startTime = System.currentTimeMillis()
		f
		System.currentTimeMillis() - startTime
	}

}