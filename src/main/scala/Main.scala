package de.aljoshavieth.redisolapclient

import de.aljoshavieth.redisolapclient.ssbqueries.{Q1_1, Q1_1_b, Q1_1_c, Q1_2, Q1_2_c, Q1_3, Q1_3_c, Q2_1, Q2_2}
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
		println("Executed in: " + calculateExecutionTime(Q1_1.execute(jedisPooled)) + "ns")

		println("Running Q1.1_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_b.execute(jedisPooled)) + "ns")


		println("Running Q1.1_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_c.execute(jedisPooled)) + "ns")

		println("Running Q1.2 ...")
		println("Executed in: " + calculateExecutionTime(Q1_2.execute(jedisPooled)) + "ns")

		println("Running Q1.2_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_c.execute(jedisPooled)) + "ns")

		println("Running Q1.3 ...")
		println("Executed in: " + calculateExecutionTime(Q1_3.execute(jedisPooled)) + "ns")

		println("Running Q1.3_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_c.execute(jedisPooled)) + "ns")

		/*

		println("Running Q1.1 ...")
		println("Executed in: " + calculateExecutionTime(Q1_1.execute(jedisPooled)) + "ns")

		println("Running Q1.2 ...")
		println("Executed in: " + calculateExecutionTime(Q1_2.execute(jedisPooled)) + "ns")

		println("Running Q1.3 ...")
		println("Executed in: " + calculateExecutionTime(Q1_3.execute(jedisPooled)) + "ns")


		println("Running Q2.1 ...")
		println("Executed in: " + calculateExecutionTime(Q2_1.execute(jedisPooled)) + "ns")


		println("Running Q2.2 ...")
		println("Executed in: " + calculateExecutionTime(Q2_2.execute(jedisPooled)) + "ns")
		*/


		jedisPipeline.close()
		jedisPooled.close()
	}



	/**
	 * This function can be used to execute any other function while measuring the execution time
	 *
	 * @param f The function to be executed
	 * @return The execution time of f in nanoseconds
	 */
	private def calculateExecutionTime(f: => Unit): Long = {
		val startTime = System.nanoTime
		f
		System.nanoTime() - startTime
	}

}