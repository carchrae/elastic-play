package models.elasticsearch;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;
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
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import play.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ElasticSearch {

	public static Node node;
	public static RestController restController;
	public static Client client;
	private static ObjectMapper mapper;

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

	private static ObjectMapper getMapper() {
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
			List<Object> objects) {

		BulkRequestBuilder bulk = getClient().prepareBulk();

		for (int i = 0; i < objects.size(); i++) {
			Object record = objects.get(i);
			Map<String, Object> recordMap = getMapper().convertValue(record,
					HashMap.class);
			String id = null;
			if (idField != null) {
				id = getMapper().convertValue(recordMap.get(id), String.class);
			}
			IndexRequestBuilder request = getClient().prepareIndex(index, type,
					id).setSource(recordMap);
			bulk.add(request);
		}
		BulkResponse bulkResponse = bulk.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			Logger.error("Failed to add elements");
		} else
			Logger.info("Imported " + objects.size() + " objects");
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

		GetResponse response = getClient().prepareGet(index, type, id)
				.execute().actionGet();

		if (!response.exists())
			return null;
		else
			return response.getSource();
	}

	private static String getJson(Object object) {
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

	public static String saveObject(String index, String type, String id,
			Object object) {
		return saveJson(index, type, id, getJson(object));
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
				.setConsistencyLevel(writeConsistency).setRefresh(refreshOnDelete ).execute().actionGet();
		if (response.notFound())
			Logger.warn("Delete failed because id was not found : " + id);
	}
}
