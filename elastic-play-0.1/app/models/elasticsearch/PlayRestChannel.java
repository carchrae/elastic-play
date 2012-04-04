package models.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.support.RestUtils;

import play.Logger;
import play.mvc.Http;
import play.mvc.Http.Header;
import play.mvc.Http.Request;
import play.mvc.Http.Response;

public class PlayRestChannel implements RestChannel {

	final RestRequest restRequest;

	final Response resp;

	public final CountDownLatch latch;
  
	public IOException sendFailure;

	public PlayRestChannel(PlayRestRequest restRequest, Response response) {
		this.restRequest = restRequest;
		this.resp = response; 
		this.latch = new CountDownLatch(1);
	}

	@Override
	public void sendResponse(RestResponse response) {
		Logger.debug("sending response");
		resp.contentType = response.contentType();
		if (RestUtils.isBrowser(restRequest.header("User-Agent"))) {
			resp.setHeader("Access-Control-Allow-Origin", "*");
			if (restRequest.method() == RestRequest.Method.OPTIONS) {
				// also add more access control parameters
				resp.setHeader("Access-Control-Max-Age", "1728000");
				resp.setHeader("Access-Control-Allow-Methods", "PUT, DELETE");
				resp.setHeader( "Access-Control-Allow-Headers",
						"X-Requested-With");
			}
		}
		String opaque = restRequest.header("X-Opaque-Id");
		if (opaque != null) {
			resp.setHeader("X-Opaque-Id", opaque);
		}
		try {
			int contentLength = response.contentLength()
					+ response.prefixContentLength()
					+ response.suffixContentLength();
//			resp.setContentLength(contentLength);

			ByteArrayOutputStream out = resp.out;
			if (response.prefixContent() != null) {
				out.write(response.prefixContent(), 0,
						response.prefixContentLength());
			}
			out.write(response.content(), 0, response.contentLength());
			if (response.suffixContent() != null) {
				out.write(response.suffixContent(), 0,
						response.suffixContentLength());
			}
			out.close();
		} catch (IOException e) {
			sendFailure = e;
		} finally {			
			latch.countDown();
		}
	}

}