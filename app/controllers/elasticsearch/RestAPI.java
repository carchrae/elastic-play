package controllers.elasticsearch;

import play.*;
import play.libs.WS;
import play.mvc.*;
import play.mvc.results.NotFound;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import models.elasticsearch.ElasticSearch;
import models.elasticsearch.PlayRestChannel;
import models.elasticsearch.PlayRestRequest;

public class RestAPI extends Controller {

	private static int counter = 0;
	private static int running = 0;

	/**
	 * Method allowing to communicate directly with an ES instance. This
	 * controller allows you to create an ElasticSearch instance inside a Play!
	 * application. You could, for example, use a Play security module to
	 * restrict access to this route.
	 * 
	 * Note that by default this route is disabled.
	 * 
	 * @param path
	 *            The queried url.
	 * @throws IOException
	 */
	public static void restServer(String path) throws IOException {

		/**
		 * be aware that enabling this route will allow complete control over
		 * your elastic search instance. adding some kind of security before
		 * doing so would be a good idea.
		 */
		if (Play.mode.isProd())
			throw new NotFound(
					"The ElasticSearch REST API is not enabled in production mode by default.");

		int requestId = counter++;

		if (Logger.isDebugEnabled())
			Logger.debug("There are " + running + " requests already running");
		running++;

		long startTime = System.currentTimeMillis();
		if (StringUtils.isEmpty(path))
			path = "/";

		PlayRestRequest restRequest = new PlayRestRequest(request, path);
		PlayRestChannel restChannel = new PlayRestChannel(restRequest,
				response);

		try {
			ElasticSearch.restController.dispatchRequest(restRequest,
					restChannel);

			if (Logger.isDebugEnabled())
				Logger.debug("Wating on request : " + requestId);

			// block until request complete
			restChannel.latch.await();
		} catch (Exception e) {
			Logger.error("Exception during request : " + requestId);
			throw new IOException("failed to dispatch request", e);
		}

		if (Logger.isDebugEnabled())
			Logger.debug("Finishing request : " + requestId);

		if (restChannel.sendFailure != null) {
			Logger.error("Failure request : " + requestId);
			throw restChannel.sendFailure;
		}

		if (Logger.isDebugEnabled()) {
			Logger.debug("Finished request : " + requestId);
			Logger.debug("Request took : "
					+ (System.currentTimeMillis() - startTime) + "ms");
		}
		running--;

		if (Logger.isDebugEnabled())
			Logger.debug("There are now " + running + " still running");

	}

}
