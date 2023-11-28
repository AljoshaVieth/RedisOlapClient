package de.aljoshavieth.redisolapclient
package clientapproach

import redis.clients.jedis.search.Document
import scala.jdk.CollectionConverters.*


object GroupByHelper {

	/**
	 * The relevantLineorderDocuments contain the properties lo_partkey and lo_orderdate. The query however needs
	 * to group by the properties p_brand1 and d_year which are present in partDocuments and dateDocuments.
	 * In order to make the groupby work, the following code replaces the properties lo_partkey and lo_orderdate
	 * of relevantLineorderDocuments with the proper properties p_brand1 and d_year from partDocuments and dateDocuments
	 */
	def updateDocuments(partDocuments: List[Document], dateDocuments: List[Document], relevantLineorderDocuments: List[Document]): List[Document] = {
		// Creating mappings from part key to brand and from order date to year
		val partKeyToBrand = partDocuments.map(doc => doc.getString("p_partkey") -> doc.getString("p_brand1")).toMap
		val dateKeyToYear = dateDocuments.map(doc => doc.getString("d_datekey") -> doc.getString("d_year")).toMap

		// Iterating over each document in relevantLineorderDocuments to update their properties
		val updatedLineorderDocuments = relevantLineorderDocuments.map { doc =>
			// Converting Java properties to a Scala Map and transforming all values to Strings
			val propertiesScalaMap = doc.getProperties.asScala.map { entry =>
				entry.getKey -> entry.getValue.toString
			}.toMap

			// Updating the properties based on the mappings created earlier
			val updatedProperties = propertiesScalaMap.flatMap {
				// If the key is lo_partkey and a mapping exists, replace it with p_brand1
				case (key, value) if key == "lo_partkey" && partKeyToBrand.contains(value) =>
					Some("p_brand1" -> partKeyToBrand(value))
				// If the key is lo_orderdate and a mapping exists, replace it with d_year
				case (key, value) if key == "lo_orderdate" && dateKeyToYear.contains(value) =>
					Some("d_year" -> dateKeyToYear(value))
				// For other keys, retain them as they are
				case (key, value) if key != "lo_partkey" && key != "lo_orderdate" =>
					Some(key -> value)
				// Discard keys that don't match any of the above cases
				case _ => None
			}

			// Creating a new Document with the updated properties
			new Document(doc.getId, updatedProperties.asJava, doc.getScore, doc.getPayload)
		}

		updatedLineorderDocuments
	}
}

