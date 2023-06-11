package de.aljoshavieth.redisolapclient
package helper

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * This Object is used to construct a RedisCommandResponse object.
 * Since jedis only returns an Object when .sendCommand() is used, a way to work with this response is needed.
 * This code requires the object to be in a certain form:
 * 	An ArrayList where the first value is a Long, the second value is an ArrayList with ByteArrays which can be casted to Strings.
 * If the object does not match this structure, exceptions are thrown.
 * The code also uses mutables which could be improved in the future.
 */
object RedisCommandResponseBuilder {
	def buildRedisCommandResponse(anyObject: Object): RedisCommandResponse = {
		var number: Long = 0
		val arguments: ListBuffer[String] = ListBuffer.empty[String]
		anyObject match {
			case arrayList: util.ArrayList[_] =>
				if (arrayList.size() >= 2) {
					val firstElement = arrayList.get(0)
					val secondElement = arrayList.get(1)
					firstElement match {
						case longValue: Long =>
							number = longValue
						case _ =>
							throw new Exception("Unexpected type for first element. Expected a Long.")
					}

					secondElement match {
						case innerArrayList: util.ArrayList[_] =>
							innerArrayList.forEach {
								case byteArray: Array[Byte] =>
									val string = new String(byteArray, StandardCharsets.UTF_8)
									arguments += string
								case _ =>
									throw new Exception("Unexpected type inside inner ArrayList. Expected ByteArray")
							}
						case _ =>
							throw new Exception("Unexpected type for second element. Expected ArrayList[ByteArray]")
					}
				} else {
					throw new Exception("Insufficient number of elements in the ArrayList. Expected 2")
				}
				RedisCommandResponse(number, arguments.toList)
			case _ =>
				throw new Exception("Unexpected type for second element. Expected ArrayList")
		}
	}
}

