package de.aljoshavieth.redisolapclient

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.jdk.CollectionConverters.*

/**
 * This object is used to format text files with ssb results from postgres to a more comparable format
 */
object FormatPostgresResults {
	def main(args: Array[String]): Unit = {
		val dir = "src/main/resources/postgresresults"
		val outputDir = "src/main/resources/formattedresults/"

		val files = Files.list(Paths.get(dir)).iterator().asScala
		files.filter(_.toString.endsWith(".txt")).foreach { path =>
			val relativePath = path.toString.replaceFirst("src/main/resources", "").dropWhile(_ == '/')
			formatPostgresResult(relativePath, outputDir)
		}
		println("Everything formatted!")
	}

	/**
	 * Formats a PostgreSQL result text file and writes the output to a new file.
	 *
	 * This function assumes that the input text file contains tabular data, where
	 * the first two lines are a header that should be skipped, and subsequent lines
	 * are data rows.
	 *
	 * Each line gets formatted and written to an output file.
	 *
	 * @param inputPath The path to the input text file containing the PostgreSQL result.
	 * @param outputDir The directory in which the output file should be created.
	 */
	private def formatPostgresResult(inputPath: String, outputDir: String): Unit = {
		println("Formatting: " + inputPath)
		val bufferedSource = Source.fromFile(inputPath)
		val lines = bufferedSource.getLines().drop(2) // Skip the first two lines
		val processedLines = lines.map(_.split("\\s*\\|\\s*").map(_.trim).mkString(" | ")).toList
		bufferedSource.close()
		val result = processedLines.mkString("\n")

		val outputFilename = outputDir + new java.io.File(inputPath).getName
		new PrintWriter(outputFilename) {
			write(result)
			close()
		}
	}

}