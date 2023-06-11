package de.aljoshavieth.redisolapclient
package helper

case class RedisCommandResponse(var number: Long,
								var values: List[String])
