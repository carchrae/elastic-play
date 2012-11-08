package models.elasticsearch;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.groovy.util.StringUtil;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.util.BeanUtil;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.BaseRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.river.twitter.TwitterRiverPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.river.twitter.TwitterRiver;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import play.Logger;
import play.cache.Cache;
import play.mvc.Http.Header;
import play.mvc.Http.Request;
import play.mvc.Scope.Session;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import controllers.elasticsearch.RestAPI;

public class ElasticSearch {

	public static Node node;
	public static RestController restController;
	public static Client client;
	public static ObjectMapper mapper;

	public static final String ES_REST_API_KEY = "es_rest_api_key";

	/**
	 * no threads is used since request is presumed to be run on same instance.
	 * more testing and tuning of these parameters is needed
	 */
	private static SearchOperationThreading threadingModel = SearchOperationThreading.NO_THREADS;
	private static SearchType searchType = SearchType.DFS_QUERY_AND_FETCH;
	private static WriteConsistencyLevel writeConsistency = WriteConsistencyLevel.ONE;
	private static boolean refreshOnDelete = true;

	public static Client getClient() {
		return client;
	}

	public static Node getNode() {
		return node;
	}

	public static ObjectMapper getMapper() {
		if (mapper == null) {
			synchronized (client) {
				mapper = new ObjectMapper();
				mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			}
		}
		return mapper;
	}

	/**
	 * efficient import of many objects
	 * 
	 * @param index
	 * @param type
	 * @param idField
	 *            - optional. if you don't specify this, id values will be
	 *            auto-generated
	 * @param objects
	 */
	public static void bulkImport(String index, String type, String idField,
			List<?> objects) {

		if (objects == null || objects.size() == 0) {
			Logger.warn("Nothing to save");
			return;
		}

		BulkRequestBuilder bulk = getClient().prepareBulk();
		for (int i = 0; i < objects.size(); i++) {
			Object record = objects.get(i);

			Map<String, Object> recordMap = convertToMap(record);
			String id = getId(idField, recordMap);

			IndexRequestBuilder request = getClient().prepareIndex(index, type,
					id).setSource(recordMap);
			bulk.add(request);
		}
		BulkResponse bulkResponse = bulk.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			Logger.error("Failed to add elements: "
					+ bulkResponse.buildFailureMessage());
		} else
			Logger.info("Imported " + objects.size() + " objects");
	}

	/**
	 * efficient update of many objects. adds new records and replaces changed
	 * ones. does not remove records.
	 * 
	 * @param index
	 * @param type
	 * @param idField
	 *            - optional. if you don't specify this, id values will be
	 *            auto-generated
	 * @param objects
	 * @return
	 */
	public static int bulkUpdate(String index, String type, String idField,
			List<?> objects) {

		if (objects == null || objects.size() == 0) {
			Logger.warn("Nothing to update");
			return 0;
		}

		int newRecords = 0;
		int updatedRecords = 0;
		List<Map<String, Object>> updates = Lists.newArrayList();
		for (int i = 0; i < objects.size(); i++) {
			Map<String, Object> recordMap = convertToMap(objects.get(i));
			String id = getId(idField, recordMap);
			Map<String, Object> current = getById(index, type, id);
			if (current == null) {
				updates.add(recordMap);
				newRecords++;
			} else if (notEqual(recordMap, current)) {
				updates.add(recordMap);
				updatedRecords++;
			}
		}

		Logger.info("There are " + newRecords + " new records");
		Logger.info("There are " + updatedRecords + " changed records");
		Logger.info("There are " + updates.size()
				+ " total updated records out of " + objects.size()
				+ " records");

		if (updates.size() == 0) {
			Logger.warn("Nothing changed, so no updates needed");
			return 0;
		}

		BulkRequestBuilder bulk = getClient().prepareBulk();
		for (Map<String, Object> recordMap : updates) {
			String id = getId(idField, recordMap);
			IndexRequestBuilder request = getClient().prepareIndex(index, type,
					id).setSource(recordMap);
			bulk.add(request);
		}
		BulkResponse bulkResponse = bulk.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			Logger.error("Failed to add elements: "
					+ bulkResponse.buildFailureMessage());
		} else
			Logger.info("Updated " + updates.size() + " objects");

		return updates.size();
	}

	private static boolean notEqual(Map<String, Object> a, Map<String, Object> b) {
		Set<String> aKeySet = a.keySet();
		Set<String> bKeySet = b.keySet();
		Collection<String> keys = CollectionUtils
				.intersection(aKeySet, bKeySet);
		if (keys.size() != aKeySet.size() || keys.size() != bKeySet.size()) {
			Logger.debug("difference in number keys");
			return true;
		}
		for (String aKey : aKeySet) {
			Object aObject = a.get(aKey);
			Object bObject = b.get(aKey);
			if (aObject == null && bObject == null)
				continue;
			else if (aObject == null && bObject != null)
				return true;
			else if (bObject == null && aObject != null)
				return true;
			else {
				if (aObject instanceof Map && bObject instanceof Map) {
					if (notEqual((Map) aObject, (Map) bObject))
						return true;
				} else {
					try {
						Class<? extends Object> type = aObject.getClass();
						Object aValue = getMapper().convertValue(aObject, type);
						Object bValue = getMapper().convertValue(bObject, type);
						if (ObjectUtils.notEqual(aValue, bValue)) {
							return true;
						}
					} catch (Exception e) {
						Logger.warn("could not compare " + aKey
								+ " value types a="
								+ aObject.getClass().getSimpleName()
								+ " and b="
								+ bObject.getClass().getSimpleName()
								+ " so presuming they are different");
						return true;
					}
				}
			}
		}
		return false;
	}

	private static String getId(String idField, Map<String, Object> recordMap) {
		String id = null;
		if (idField != null) {
			id = getMapper().convertValue(recordMap.get(idField), String.class);
		}
		return id;
	}

	private static Map<String, Object> convertToMap(Object record) {
		Map<String, Object> recordMap;
		if (record instanceof Map)
			recordMap = (Map<String, Object>) record;
		else
			recordMap = getMapper().convertValue(record, HashMap.class);
		return recordMap;
	}

	private static void convertToMap(String[] srcFields, String[] destFields,
			List<Map<String, Object>> results, SearchResponse response) {
		SearchHits hits = response.getHits();
		for (SearchHit hit : hits) {
			if (srcFields == null && destFields == null)
				results.add(hit.getSource());
			else {
				HashMap<String, Object> result = new HashMap<String, Object>();
				for (int i = 0; i < srcFields.length; i++) {
					String fieldName = srcFields[i];
					Map<String, Object> doc = hit.getSource();
					Object value = doc.get(fieldName);
					result.put(destFields[i], String.valueOf(value));
				}
				results.add(result);
			}

		}
	}

	/**
	 * executes a search query and returns a map of results
	 * 
	 * @param index
	 * @param srcFields
	 * @param destFields
	 * @param query
	 * @param from
	 * @param limit
	 * @param explain
	 * @return
	 */
	public static List<Map<String, Object>> executeQuery(String index,
			String[] srcFields, String[] destFields, QueryBuilder query,
			int from, int limit, boolean explain) {
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		try {
			SearchRequestBuilder search = client.prepareSearch(index);
			search.setQuery(query).setExplain(explain).setFrom(from);

			/**
			 * note the default elastic search result size is small (10 hits I
			 * think)
			 */
			if (limit != 0)
				search.setSize(limit);

			search.setOperationThreading(threadingModel).setSearchType(
					searchType);

			SearchResponse response = search.execute().actionGet();
			convertToMap(srcFields, destFields, results, response);

		} catch (Exception e) {
			Logger.error(e.getMessage());
			e.printStackTrace();
		}
		return results;
	}

	public static Map<String, Object> getById(String index, String type,
			String id) {
		try {

			GetResponse response = getClient().prepareGet(index, type, id)
					.execute().actionGet();

			if (!response.exists())
				return null;
			else
				return response.getSource();
		} catch (Exception e) {
			e.printStackTrace();
			Logger.error(e.getMessage());
			return null;
		}
	}

	public static String getJson(Object object) {
		String json = null;
		try {
			ObjectMapper mapper = getMapper();
			json = mapper.writeValueAsString(object);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}

	public static <T> T getObject(String index, String type, String id,
			Class<T> clazz) {
		try {
			Map<String, Object> map = getById(index, type, id);
			ObjectMapper mapper = getMapper();
			String json = mapper.writeValueAsString(map);
			return mapper.readValue(json, clazz);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String saveJson(String index, String type, String id,
			String data) {
		IndexRequestBuilder request = getClient().prepareIndex(index, type);
		if (id != null && !StringUtils.isBlank(id))
			request.setId(id);
		request.setSource(data);
		request.setConsistencyLevel(writeConsistency);
		return request.execute().actionGet().id();
	}

	public static String saveMap(String index, String type, String id,
			Map<String, Object> data) {
		IndexRequestBuilder request = getClient().prepareIndex(index, type);
		if (id != null && !StringUtils.isBlank(id))
			request.setId(id);
		request.setSource(data);
		request.setConsistencyLevel(writeConsistency);
		return request.execute().actionGet().id();
	}

	public static void createIndex(String index) {
		getClient().prepareIndex(index, "").setCreate(true);
	}

	public static String saveObject(String index, String type, String id,
			Object object) {
		String json = getJson(object);
		Logger.debug("Saving: " + json);
		return saveJson(index, type, id, json);
	}

	public static void setFieldAsGeoPoint(String fieldName) {
		setFieldAsType(fieldName, "geo_point");
	}

	public static void setFieldAsType(String fieldName, String type) {
		PutMappingRequestBuilder request = getClient().admin().indices()
				.preparePutMapping(new String[] { fieldName }).setType(type);
		request.execute().actionGet();
	}

	/**
	 * this method allows you to specify many type properties of a field. Note
	 * you must specify properties before you add any data to an index. it is
	 * not possible to change the type after data has been added.
	 * 
	 * @param index
	 * @param type
	 * @param field
	 * @param fieldProperties
	 */
	public static void setType(String index, String type, String field,
			HashMap<String, String> fieldProperties) {
		HashMap<String, Object> properties = new HashMap<String, Object>();
		properties.put(field, fieldProperties);
		HashMap<String, Object> mappingSource = new HashMap<String, Object>();
		mappingSource.put(type, properties);

		PutMappingRequestBuilder request = getClient().admin().indices()
				.preparePutMapping(index).setType(type)
				.setSource(mappingSource);

		request.execute().actionGet();
	}

	public static void delete(String index, String type, String id) {
		DeleteResponse response = ElasticSearch.client
				.prepareDelete(index, type, id)
				.setConsistencyLevel(writeConsistency)
				.setRefresh(refreshOnDelete).execute().actionGet();
		if (response.notFound())
			Logger.warn("Delete failed because id was not found : " + id);
	}

	public static SearchResponse findAll(int from, String... indices) {
		try {
			QueryBuilder query;
			query = QueryBuilders.matchAllQuery();
			SearchResponse searchResponse = ElasticSearch.getClient()
					.prepareSearch(indices).setQuery(query).setFrom(from)
					.execute().actionGet();
			return searchResponse;
		} catch (Exception e) {
			Logger.error(e.getMessage());
			return null;
		}
	}

	public static SearchResponse search(String term, int from,
			String... indicies) {
		try {
			QueryBuilder query = QueryBuilders.wildcardQuery("_all",
					term.toLowerCase());
			SearchResponse searchResponse = ElasticSearch.getClient()
					.prepareSearch(indicies).setQuery(query).setFrom(from)
					.execute().actionGet();
			return searchResponse;
		} catch (Exception e) {
			Logger.error(e.getMessage());
			return null;
		}
	}

	public static void setRestApiAccess(boolean allow) {
		Session current = Session.current();
		if (current == null)
			Logger.error("No current request!");
		else {
			Logger.debug("setRestApiAccess - "
					+ (allow ? "allowed" : "NOT ALLOWED") + " for session"
					+ current.getId());
			Cache.set(ES_REST_API_KEY + Session.current().getId(), allow);
		}
	}

	public static void setRestApiAccess(boolean allow, String key) {
		Logger.debug("setRestApiAccess - "
				+ (allow ? "allowed" : "NOT ALLOWED") + " for token " + key);
		Cache.set(ES_REST_API_KEY + Session.current().getId(), allow);
	}

	public static boolean getRestApiAccess() {
		Session current = Session.current();
		if (current == null) {
			Logger.error("getRestApiAccess - no session or request");
			return false;
		}

		String key = current.getId();

		return getRestApiAccess(key);
	}

	public static boolean getRestApiAccess(String key) {

		boolean allowed = BooleanUtils.isTrue(Cache.get(ES_REST_API_KEY + key,
				Boolean.class));
		Logger.debug("getRestApiAccess - " + (allowed ? "OK" : "NOT ALLOWED")
				+ " for session" + key);

		return allowed;
	}

}
