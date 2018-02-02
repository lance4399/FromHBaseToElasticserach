package com.sdg.userprofile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;

public class ESPersistor {
	private TransportClient transportclient;
	private Client client;
	private BulkProcessor bulkProcessor;
	
	public static void main(String[] args) {		
		System.out.println(System.getProperty("java.class.path"));
		try {
			new ESPersistor().initialize();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public void initialize() throws NumberFormatException, UnknownHostException {
		System.out.println("begin initESPersistor.....");
		initBulkProcessor();
		System.out.println("finish initESPersistor.....");
	}

	private void initBulkProcessor() throws NumberFormatException, UnknownHostException {
		String brokerList ="10.128.113.111:9300,10.128.113.112:9300,10.128.113.113:9300,10.128.113.114:9300,10.128.113.115:9300";
		String clusterName ="rtc-search";
		int bulkActions = 5000;
		int bulkSize = 5242881;
		int bulkFlushInterval = 3;
		int bulkConcurrentRequests = 3;
		List<TransportAddress> addressList = new ArrayList<TransportAddress>();
		for (String hostAndPort : brokerList.split(",")) {
			addressList.add(getInetSocketTransportAddress(hostAndPort));
		}
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		transportclient = new PreBuiltTransportClient(settings);
		client = transportclient.addTransportAddresses(addressList.toArray(new TransportAddress[] {}));
		bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			private Map<Long, Long> startMap = Maps.newHashMap();

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
//				System.out.println(String.format("Begin Bulk: executionId = %s, Count = %s, Size = %s", executionId, request.numberOfActions(), request.estimatedSizeInBytes()));
				startMap.put(executionId, System.currentTimeMillis());
			} 

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				if (response.hasFailures()) {
					logError(executionId, request, response, null);
				} else {
					Long start = startMap.remove(executionId);
//					System.out.println(String.format("Finish Bulk, executionId = %s, Cost = %sms, Count = %s, Size = %s",
//							executionId, start == null ? -1 : (System.currentTimeMillis() - start),
//							request.numberOfActions(), request.estimatedSizeInBytes()));
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				logError(executionId, request, null, failure);
			}

			private void logError(long executionId, BulkRequest request, BulkResponse response, Throwable failure) {
				System.out.println(String.format("Error executing Bulk, executionId = %s, Count = %s, Size = %s", executionId,
						request.numberOfActions(), request.estimatedSizeInBytes()));
				startMap.remove(executionId);
				List<ActionRequest> reqList = request.requests();
				if (response == null) {
					// 非服务器端异常
					System.out.println("Error: client side exception: "+ failure.toString());
					for (ActionRequest ar : reqList) {
						IndexRequest ir = (IndexRequest) ar;
						String errorRecord = new JSONObject(ir.sourceAsMap()).toJSONString();
						System.out.println(String.format("INDEX = %s, FAILED RECORD = %s", ir.index(), errorRecord));
					}
				} else {
					// 服务器端异常
					for (BulkItemResponse bir : response.getItems()) {
						if (bir.isFailed()) {
							IndexRequest ir = (IndexRequest) reqList.get(bir.getItemId());
							String errorRecord = new JSONObject(ir.sourceAsMap()).toJSONString();
							String errorMessage = bir.getFailure().getCause().getMessage();
							System.out.println(String.format("INDEX = %s, FAILED RECORD = %s, ERROR MESSAGE = %s", ir.index(),
									errorRecord, errorMessage));
						}
					}
				}
			}
		}).setBulkActions(bulkActions).setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.BYTES))
				.setFlushInterval(TimeValue.timeValueSeconds(bulkFlushInterval))
				.setConcurrentRequests(bulkConcurrentRequests).build();
	}

	private InetSocketTransportAddress getInetSocketTransportAddress(String hostAndPort) throws NumberFormatException, UnknownHostException {
		String host = hostAndPort.split(":")[0];
		String port = hostAndPort.split(":")[1];
		return new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port));
	}

	public void persist(String entityName, String key, String source) {
		// 真实索引名 = entityName-yyyyMMdd
		// "读"别名 = entityName
		// "写"别名 = entityName$
		bulkProcessor.add(new IndexRequest(entityName + "$", "data", null).source(source));
	}

	public void close() {
		System.out.println("Closing bulkProcessor...");
		try {
			bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Closing ESClient...");
		client.close();
		transportclient.close();
	}

}

	