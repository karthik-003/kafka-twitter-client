package com.karvis.kt.consumer.twitterconsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.karvis.kt.consumer.elasticsearch.ElasticSearchConsumer;

public class TwitterConsumer {

	public static String bootstrapServer = "127.0.0.1:9092";
	public static String groupId = "twitter-consumer-elasticsearch-group";
	
	static KafkaConsumer<String, String> createClient(String topic) {
		
		Properties props = new Properties();
		
		//Basic props
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		
		//Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
	
	public static void main(String[] args) throws IOException {
		KafkaConsumer<String, String> consumer = createClient("kafka-twitter-tweets");
		RestHighLevelClient highLevelClient = ElasticSearchConsumer.createClient();
		
		while(true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			System.out.println("Recieved: "+consumerRecords.count()+".");

			BulkRequest bulkRequest = new BulkRequest();
			for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				String id = consumerRecord.topic()+"_"+consumerRecord.partition()+"_"+consumerRecord.offset();
				System.out.println("ID: "+id);
				//ElasticSearchConsumer.createTweet(consumerRecord.value(),id);
				
				//BulkRequest to speed up the consumer
				
				
				IndexRequest indexReq = new IndexRequest("twitter", "tweets",id)
						.source(consumerRecord.value(),XContentType.JSON);
				
				bulkRequest.add(indexReq);
				
				
				
				
			}
			BulkResponse resp = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
			System.out.println("Committing offsets...");
			consumer.commitSync();
			System.out.println("Offsets comitted!!");
			highLevelClient.close();
			/*
			 * try { Thread.sleep(100); } catch (InterruptedException e) { // TODO
			 * Auto-generated catch block e.printStackTrace(); }
			 */
		}
		
	}
}
