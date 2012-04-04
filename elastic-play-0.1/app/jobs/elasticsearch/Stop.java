package jobs.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.*;

import models.elasticsearch.ElasticSearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import play.Logger;
import play.Play;
import play.jobs.Job;
import play.jobs.OnApplicationStart;
import play.jobs.OnApplicationStop;

@OnApplicationStop
public class Stop extends Job {

	@Override
	public void doJob() {
		try {
			Logger.info("Stopping ElasticSearch node");
			ElasticSearch.node.close();
			Logger.info("Stopped ElasticSearch node");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
