package com.karvis.kt.consumer.elasticsearch;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

	static Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class);
	static BulkRequest bulk;
	public static RestHighLevelClient createClient() {
		
		//https://9xa268tzpz:hi6zc9yz88@karvis-kafka-cluster-5847518766.ap-southeast-2.bonsaisearch.net:443
		String hostName = "karvis-kafka-cluster-5847518766.ap-southeast-2.bonsaisearch.net";
		String userName = "9xa268tzpz";
		String password = "hi6zc9yz88";
		
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY	, new UsernamePasswordCredentials(userName, password));
		
		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostName,443,"https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder arg0) {
						// TODO Auto-generated method stub
						return arg0.setDefaultCredentialsProvider(credentialProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		
		return client;
	}
	
	@SuppressWarnings("deprecation")
	public static void createTweet(String jsonString,String tweetId) throws IOException {
		RestHighLevelClient highLevelClient = createClient();
		
		IndexRequest indexReq = new IndexRequest("twitter", "tweets",tweetId)
				.source(jsonString,XContentType.JSON);
		
		IndexResponse idxResp = highLevelClient.index(indexReq, RequestOptions.DEFAULT);
		String id = idxResp.getId();
		log.info("ID: "+id);
		
		highLevelClient.close();
	}
	
	public static void createTweetBulk() {
		if(bulk == null) {
			bulk = new BulkRequest();
		}
		RestHighLevelClient highLevelClient = createClient();
	}
	public static void main(String[] args) throws IOException {
		
		
		String jsonString = "{\"foo\":\"bar\"}";
		//createTweet(jsonString);
		
	}
}
