package de.aljoshavieth.redisolapclient
package clientapproach

import redis.clients.jedis.JedisPooled
import redis.clients.jedis.search.{Document, Query}

import scala.jdk.CollectionConverters.*


/**
 * This abstract class is the foundation of all Queries sent to redis.
 * It´s name is "RedisQuery" to avoid confusions with redis.clients.jedis.search.Query
 */
abstract class RedisQuery {
	def execute(jedisPooled: JedisPooled): Unit

	/**
	 * This method generalises running queries against Redis to retrieve a list of documents
	 *
	 * @param jedisPooled  An JedisPooled instance that is used to communicate with Redis
	 * @param indexName    The index that should be used
	 * @param query        The query that should be used. If only NUMERIC fields are used, an empty query is enough.
	 * @param filters      A list of Query.Filter objects to filter the results
	 * @param returnFields The fields that should be returned. This can be used to ignore irrelevant fields.
	 * @return A List[Document] with all relevant Documents found by the query
	 */
	protected def queryDocuments(jedisPooled: JedisPooled, indexName: String, query: Query = new Query(), filters: List[Query.Filter] = List.empty, returnFields: List[String]): List[Document] = {
		query
			.limit(0, Integer.MAX_VALUE) // Set the limit of results as high as possible
			.returnFields(returnFields: _*) // Define which fields should be included in the Document objects
			.timeout(Integer.MAX_VALUE) // Make sure to enable as much time as possible to the Query so it can get as much results as possible TODO: can maybe disabled with setting it to 0

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
	protected def filterDocuments(documentsToFilter: List[Document], filterField1: String, documentsToFilterWith: List[Document], filterField2: String): List[Document] = {
		val filterValues = documentsToFilterWith.map(_.getString(filterField2)).toSet // Converting to set to enable faster comparison due to constant-time lookups
		documentsToFilter.filter(doc => filterValues.contains(doc.getString(filterField1)))
	}

	protected def filterAndJoinDocuments(documentsToFilter: List[Document], filterField1: String, documentsToFilterWith: List[Document], filterField2: String, fieldsToJoin: List[String]): List[Document] = {
		//val filterValues = documentsToFilterWith.map(_.getString(filterField2)).toSet // Converting to set to enable faster comparison due to constant-time lookups
		
		//val documentsToFilterWithMap = documentsToFilterWith.groupBy(_.getString(filterField2)).view.mapValues(_.head)
		//documentsToFilter.filter(doc => filterValues.contains(doc.getString(filterField1)))
		
		val filterMap = documentsToFilterWith.map(doc => (doc.getString(filterField2), doc)).toMap
		documentsToFilter
			.filter(doc => filterMap.contains(doc.getString(filterField1)))
			.map(doc => {
				val fieldsToJoinValues = fieldsToJoin.map(filterMap(doc.getString(filterField1)).getString)
				fieldsToJoin.zip(fieldsToJoinValues).foldLeft(doc)((d,p) => d.set(p._1, p._2))
			})
		
	}
}
