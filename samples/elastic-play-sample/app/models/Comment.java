package models;

import models.elasticsearch.ElasticSearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class Comment {

	public String author;
	public String content;

	public static SearchHits findAll(int from) {
		QueryBuilder query;
		query = QueryBuilders.matchAllQuery();
		SearchResponse searchResponse = ElasticSearch.getClient()
				.prepareSearch("comments").setQuery(query).setFrom(from)
				.execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		return hits;
	}

	public static SearchHits search(String term, int from) {
		QueryBuilder query = QueryBuilders.wildcardQuery("_all", term.toLowerCase());
		SearchResponse searchResponse = ElasticSearch.getClient()
				.prepareSearch("comments").setQuery(query).setFrom(from)
				.execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		return hits;
	}

}
