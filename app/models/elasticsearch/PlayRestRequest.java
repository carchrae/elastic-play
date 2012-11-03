/*
 * This code has been modified by Tom Carchrae to work with 
 * Play! 1.2.x request and response objects.  Original source 
 * code is here:
 *  
 *  https://github.com/elasticsearch/elasticsearch-transport-wares/tree/master/src/main/java/org/elasticsearch/wares
 */

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package models.elasticsearch;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.rest.support.AbstractRestRequest;
import org.elasticsearch.rest.support.RestUtils;

import play.libs.IO;
import play.mvc.Http.Header;
import play.mvc.Http.Request;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PlayRestRequest extends AbstractRestRequest implements
		org.elasticsearch.rest.RestRequest {

	private final Request playRequest;

	private final Method method;

	private final Map<String, String> params;

	private final byte[] content;

	private String path;

	public PlayRestRequest(Request request, String path, String body)
			throws IOException {
		this.playRequest = request;
		this.path = path;
		this.method = Method.valueOf(request.method);
		this.params = new HashMap<String, String>();

		if (request.querystring != null) {
			RestUtils.decodeQueryString(request.querystring, 0, params);
		}

		body = body == null ? "" : body;
		/**
		 * strip out ctrl chars that break jackson
		 */
		body = StringUtils.replaceChars(body, "\n\t", "  ");
		content = body.getBytes();
	}

	@Override
	public Method method() {
		return this.method;
	}

	@Override
	public String uri() {
		return path;
		// return playRequest.path;
		// return playRequest.getRequestURI().substring(
		// playRequest.getContextPath().length()
		// + playRequest.getServletPath().length());
	}

	@Override
	public String rawPath() {
		return path;
		// return playRequest.getRequestURI().substring(
		// playRequest.getContextPath().length()
		// + playRequest.getServletPath().length());
	}

	@Override
	public boolean hasContent() {
		return content.length > 0;
	}

	@Override
	public boolean contentUnsafe() {
		return false;
	}

	@Override
	public BytesReference content() {
		return new BytesArray(content);
	}

	@Override
	public String header(String name) {
		Header header = playRequest.headers.get(name);
		if (header != null)
			return header.value();
		else
			return null;
	}

	@Override
	public Map<String, String> params() {
		return params;
	}

	@Override
	public boolean hasParam(String key) {
		return params.containsKey(key);
	}

	@Override
	public String param(String key) {
		return params.get(key);
	}

	@Override
	public String param(String key, String defaultValue) {
		String value = params.get(key);
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

}