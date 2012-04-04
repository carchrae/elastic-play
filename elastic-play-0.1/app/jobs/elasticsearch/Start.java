package jobs.elasticsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

import models.elasticsearch.ElasticSearch;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestController;

import play.Logger;
import play.Play;
import play.jobs.Job;
import play.jobs.OnApplicationStart;

@OnApplicationStart
public class Start extends Job {

	@Override
	public void doJob() {

		if (ElasticSearch.node == null) {
			Logger.info("Initializing elasticsearch Node");
			ImmutableSettings.Builder settings = ImmutableSettings
					.settingsBuilder();

			try {
				// ability to switch to local instance in dev mode

				String applicationConf = null;

				if (Play.mode.isDev()) {
					applicationConf = "conf/elasticsearch.dev.json";
					Logger.info("DEV MODE : using local instance configuration for elastic search : "
							+ applicationConf);
				}

				if (applicationConf != null
						&& !new File(applicationConf).exists()) {
					Logger.warn("Dev Mode configuration does not exist.  Trying to use production index instead");
				}

				if (applicationConf == null)
					applicationConf = "conf/elasticsearch.prod.json";

				InputStream resourceAsStream = new FileInputStream(
						applicationConf);
				if (resourceAsStream != null) {
					settings.loadFromStream("/WEB-INF/elasticsearch.json",
							resourceAsStream);
					try {
						resourceAsStream.close();
					} catch (IOException e) {
						System.err.println("Could not load settings");
					}
				}
			} catch (Exception e) {
				System.err.println("Could not load settings");
			}

			if (settings.get("http.enabled") == null) {
				settings.put("http.enabled", false);
			}

			if (settings.get("node.local") == null) {
				settings.put("node.local", true);
			}

			ElasticSearch.node = NodeBuilder.nodeBuilder().settings(settings)
					.node();
			
			ElasticSearch.restController = ((InternalNode) ElasticSearch.node)
					.injector().getInstance(RestController.class);

			ElasticSearch.client = ElasticSearch.node.client();

			try {
				Logger.info("Waiting for elastic search cluster to start");
				// yellow status is good enough to serve requests
				ElasticSearch.client.admin().cluster().prepareHealth()
						.setWaitForYellowStatus().execute().get();
				Logger.info("Elastic search cluster has started");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			Logger.info("bound_address : "
					+ ElasticSearch.node.settings().get("bound_address"));

		}

	}
}
