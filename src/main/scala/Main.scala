package de.aljoshavieth.redisolapclient

import clientapproach.*
import serverapproach.LuaScriptLoader

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

		//runClientApproachQueries()
		runServerApproachQueries()

		jedisPipeline.close()
		jedisPooled.close()
	}

	private def runServerApproachQueries(): Unit = {
		println(jedisPooled.fcall("querySpecificDocuments", List[String]().asJava, List[String]().asJava))
	}

	private def runClientApproachQueries(): Unit = {

		/*

		println("\n----------------------------------------")
		println("Running Q1.1 ...")
		println("Executed in: " + calculateExecutionTime(Q1_1.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q1.1_b ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_b.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q1.1_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_1_c.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q1.2 ...")
		println("Executed in: " + calculateExecutionTime(Q1_2.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q1.2_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_2_c.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q1.3 ...")
		println("Executed in: " + calculateExecutionTime(Q1_3.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q1.3_c ...")
		println("Executed in: " + calculateExecutionTime(Q1_3_c.execute(jedisPooled)) + "ns")

		*/

		println("\n----------------------------------------")
		println("Running Q2.1 ...")
		println("Executed in: " + calculateExecutionTime(Q2_1.execute(jedisPooled)) + "ns")

		println("\n----------------------------------------")
		println("Running Q2.1_c ...")
		println("Executed in: " + calculateExecutionTime(Q2_1_c.execute(jedisPooled)) + "ns")


		/*
		println("\n----------------------------------------")
		println("Running Q2.2 ...")
		println("Executed in: " + calculateExecutionTime(Q2_2.execute(jedisPooled)) + "ns")
		*/


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