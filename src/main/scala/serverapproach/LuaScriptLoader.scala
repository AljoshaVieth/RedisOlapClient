package de.aljoshavieth.redisolapclient
package serverapproach

object LuaScriptLoader {
	def loadLuaScript(functionFilePath: String): String = {
		val source = scala.io.Source.fromFile(functionFilePath)
		val functionContent = try source.mkString finally source.close()
		functionContent
	}
}
