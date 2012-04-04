package controllers;

import play.*;
import play.data.validation.Required;
import play.mvc.*;
import play.mvc.Scope.Session;
import play.mvc.results.NotFound;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.codehaus.groovy.util.StringUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import models.*;
import models.elasticsearch.ElasticSearch;

public class Application extends Controller {

	public static void index() {
		render();
	}

	public static void delete(String id) {
		ElasticSearch.delete("comments", "comment", id);
		
		search(Session.current().get("term"), 0);
	}

	public static void edit(String id) {
		Comment comment = null;

		if (id != null)
			comment = ElasticSearch.getObject("comments", "comment", id,
					Comment.class);

		if (id != null && comment == null)
			throw new NotFound("Could not find id = " + id);

		render(id, comment);
	}

	public static void search(String term, int from) {
		Session.current().put("term", term);
		SearchHits searchHits;
		if (!StringUtils.isBlank(term)) {
			searchHits = Comment.search(term, from);
		} else {
			searchHits = Comment.findAll(from);
		}

		render(searchHits, term);
	}

	public static void add(String id, String author, String content) {
		Comment comment = new Comment();
		comment.author = author;
		comment.content = content;
		id = ElasticSearch.saveObject("comments", "comment", id, comment);
		edit(id);
	}

}