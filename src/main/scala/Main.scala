package de.aljoshavieth.redisolapclient

import clientapproach.*
import clientapproach.q1_1.*
import clientapproach.q1_2.*
import clientapproach.q1_3.*
import clientapproach.q2_1.{Q2_1_client_a, Q2_1_client_b}
import clientapproach.q2_2.{Q2_2_client_a, Q2_2_client_b}
import clientapproach.q2_3.{Q2_3_client_a, Q2_3_client_b}
import denormalizedapproach.*
import scanapproach.q1_1.Q1_1_scan_a
import scanapproach.q1_2.Q1_2_scan_a
import serverapproach.LuaScriptLoader
import serverapproach.q1_1.{Q1_1_server_a, Q1_1_server_b, Q1_1_server_c, Q1_1_server_d}
import serverapproach.q1_2.{Q1_2_server_a, Q1_2_server_b, Q1_2_server_c, Q1_2_server_d}
import serverapproach.q1_3.Q1_3_server_a
import serverapproach.q2_1.Q2_1_server_a

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.search.SearchProtocol.SearchCommand
import redis.clients.jedis.search.aggr.AggregationResult
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
		//runDenormalizedApproachQueries()
		//runClientApproachQueries()
		runServerApproachQueries()
		//runScanApproachQueries()
		//runScanVsSearchQueries()

		jedisPipeline.close()
		jedisPooled.close()
	}

	private def runDenormalizedApproachQueries(): Unit = {
		println("\n----------------------------------------")
		println("Running denormalized-based queries ...")
		println("........................................")


		var result: AggregationResult = null
		var formattedResult: String = ""

		// Q 1.1
		println("\nRunning Q1.1 denormalized ...")
		result = Q1_1_denormalized.execute(jedisPooled)
		formattedResult = Q1_1_denormalized.toComparableString(result)
		println(formattedResult)
		printResultIsCorrect(Q1_1_denormalized.isCorrect(formattedResult))

		// Q 1.2
		println("\nRunning Q1.2 denormalized ...")
		result = Q1_2_denormalized.execute(jedisPooled)
		formattedResult = Q1_2_denormalized.toComparableString(result)
		printResultIsCorrect(Q1_2_denormalized.isCorrect(formattedResult))

		// Q 1.3
		println("\nRunning Q1.3 denormalized ...")
		result = Q1_3_denormalized.execute(jedisPooled)
		formattedResult = Q1_3_denormalized.toComparableString(result)
		printResultIsCorrect(Q1_3_denormalized.isCorrect(formattedResult))


		// Q 2.1
		println("\nRunning Q2.1 denormalized ...")
		result = Q2_1_denormalized.execute(jedisPooled)
		formattedResult = Q2_1_denormalized.toComparableString(result)
		printResultIsCorrect(Q2_1_denormalized.isCorrect(formattedResult))

		// Q 2.2
		println("\nRunning Q2.2 denormalized ...")
		result = Q2_2_denormalized.execute(jedisPooled)
		formattedResult = Q2_2_denormalized.toComparableString(result)
		printResultIsCorrect(Q2_2_denormalized.isCorrect(formattedResult))

		// Q 2.3
		println("\nRunning Q2.3 denormalized ...")
		result = Q2_3_denormalized.execute(jedisPooled)
		formattedResult = Q2_3_denormalized.toComparableString(result)
		printResultIsCorrect(Q2_3_denormalized.isCorrect(formattedResult))


		// Q 3.1
		println("\nRunning Q3.1 denormalized ...")
		result = Q3_1_denormalized.execute(jedisPooled)
		formattedResult = Q3_1_denormalized.toComparableString(result)
		printResultIsCorrect(Q3_1_denormalized.isCorrect(formattedResult))

		// Q 3.2
		println("\nRunning Q3.2 denormalized ...")
		result = Q3_2_denormalized.execute(jedisPooled)
		formattedResult = Q3_2_denormalized.toComparableString(result)
		printResultIsCorrect(Q3_2_denormalized.isCorrect(formattedResult))

		// Q 3.3
		println("\nRunning Q3.3 denormalized ...")
		result = Q3_3_denormalized.execute(jedisPooled)
		formattedResult = Q3_3_denormalized.toComparableString(result)
		printResultIsCorrect(Q3_3_denormalized.isCorrect(formattedResult))

		// Q 3.4
		println("\nRunning Q3.4 denormalized ...")
		result = Q3_4_denormalized.execute(jedisPooled)
		formattedResult = Q3_4_denormalized.toComparableString(result)
		printResultIsCorrect(Q3_4_denormalized.isCorrect(formattedResult))


		// Q 4.1
		println("\nRunning Q4.1 denormalized ...")
		result = Q4_1_denormalized.execute(jedisPooled)
		formattedResult = Q4_1_denormalized.toComparableString(result)
		printResultIsCorrect(Q4_1_denormalized.isCorrect(formattedResult))

		// Q 4.2
		println("\nRunning Q4.2 denormalized ...")
		result = Q4_2_denormalized.execute(jedisPooled)
		formattedResult = Q4_2_denormalized.toComparableString(result)
		printResultIsCorrect(Q4_2_denormalized.isCorrect(formattedResult))

		// Q 4.3
		println("\nRunning Q4.3 denormalized ...")
		result = Q4_3_denormalized.execute(jedisPooled)
		formattedResult = Q4_3_denormalized.toComparableString(result)
		printResultIsCorrect(Q4_3_denormalized.isCorrect(formattedResult))
	}

	private def printResultIsCorrect(result: Boolean): Unit =
		println(if (result) "Result is correct!" else "Result is incorrect!!!")

	private def runServerApproachQueries(): Unit = {
		println("\n----------------------------------------")
		println("Running server-based queries ...")
		println("........................................")


		// Q 1.1
		println("\n    Q1.1\n")
		println("Running Q1.1 server_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_a.execute(jedisPooled)) + "ms\n")

		// Q 1.2
		println("\n    Q1.2\n")
		println("Running Q1.2 server_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_a.execute(jedisPooled)) + "ms\n")

		// Q 1.3
		println("\n    Q1.3\n")
		println("Running Q1.3 server_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_server_a.execute(jedisPooled)) + "ms\n")

		// The following code is not functional since the corresponding lua code is not working or was not developed further
		// due to this approach being declared as to slow to compete
		/*
		// Q 2.1
		println("\n    Q2.1\n")
		println("Running Q2.1 server_a ...")
		println("Executed in: " + calculateExecutionTime(Q2_1_server_a.execute(jedisPooled)) + "ms\n")
		*/
		/*
		println("Running Q1.1 server_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_b.execute(jedisPooled)) + "ms\n")
		*/
		/*
		println("Running Q1_1 server_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_c.execute(jedisPooled)) + "ms\n")
		println("Running Q1.1 server_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_server_d.execute(jedisPooled)) + "ms\n")
		*/
    
		/*
		println("Running Q1.2 server_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_b.execute(jedisPooled)) + "ms\n")
		*/
		/*
		println("Running Q1.2 server_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_c.execute(jedisPooled)) + "ms\n")

		println("Running Q1.2 server_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_server_d.execute(jedisPooled)) + "ms\n")
		*/
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


		println("Running Q1.1 client_d_alternative ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_client_d_alternative.execute(jedisPooled)) + "ms\n")

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

		println("Running Q1.2 client_d_alternative ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_client_d_alternative.execute(jedisPooled)) + "ms\n")

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

		println("Running Q1.3 client_d_alternative ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_d_alternative.execute(jedisPooled)) + "ms\n")

		println("Running Q1.3 client_d ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_d.execute(jedisPooled)) + "ms\n")

		println("Running Q1.3 client_e ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_client_e.execute(jedisPooled)) + "ms\n")



		// Q 2.1
		println("\n    Q2.1\n")
		println("Running Q2.1_a ...")
		println("Executed in: " + calculateExecutionTime(Q2_1_client_a.execute(jedisPooled)) + "ms\n")


		println("\n----------------------------------------")
		println("Running Q2.1_b ...")
		println("Executed in: " + calculateExecutionTime(Q2_1_client_b.execute(jedisPooled)) + "ms")

		// Q 2.2
		println("\n----------------------------------------")
		println("Running Q2.2_a ...")
		println("Executed in: " + calculateExecutionTime(Q2_2_client_a.execute(jedisPooled)) + "ms\n")

		println("\n----------------------------------------")
		println("Running Q2.2_b ...")
		println("Executed in: " + calculateExecutionTime(Q2_2_client_b.execute(jedisPooled)) + "ms\n")

		// Q 2.5
		println("\n----------------------------------------")
		println("Running Q2.3_a ...")
		println("Executed in: " + calculateExecutionTime(Q2_3_client_a.execute(jedisPooled)) + "ms\n")

		println("\n----------------------------------------")
		println("Running Q2.3_b ...")
		println("Executed in: " + calculateExecutionTime(Q2_3_client_b.execute(jedisPooled)) + "ms\n")

	}

	// This approach has been abandoned since it needs pre-calculated data
	private def runScanApproachQueries(): Unit = {
		val jedis = new Jedis("localhost", 6379)
		jedis.select(1)
		jedis.getClient.setTimeoutInfinite()


		println("\n----------------------------------------")
		println("Running scan-based queries ...")
		println("........................................")

		// Q 1.1 a
		println("\n    Q1.1\n")
		println("Running Q1.1 scan_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_scan_a.execute(jedis)) + "ms\n")

		// Q 1.1 b
		println("\n    Q1.1_b\n")
		println("Running Q1.1 scan_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_scan_a.execute(jedis)) + "ms\n")

		// Q 1.2
		println("\n    Q1.2\n")
		println("Running Q1.2 scan_a ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_scan_a.execute(jedis)) + "ms\n")
	}

	/**
	 * Configure redis by deactivating limits and timeouts as well as loading the lua lib
	 */
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