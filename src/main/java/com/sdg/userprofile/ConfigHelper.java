package com.sdg.userprofile;
import java.util.Locale;
import java.util.ResourceBundle;

public class ConfigHelper {
	/**
	 * Flume
	 */
	public static String Flume_HOST;
	public static String Flume_PORT;
	/**
	 * Kafka
	 */
	public static String Kafka_HOST;
	public static String Kafka_PORT;

	/**
	 * HBase
	 */
	public static String HBase_Zookeeper_QUORUM;
	public static String HBase_Zookeeper_QUORUM_HOST;
	public static String HBase_Zookeeper_CLIENTPORT;
	public static String HBase_ZOOKEEPER_PORT;

	/**
	 * Cassandra
	 */
	public static String Cassandra_Presto_HOST;
	public static String Cassandra_Presto_PORT;
	public static String Presto_Driver;
	
	public static String Presto_HOST;
	/**
	 * Elasticsearch
	 */
	public static String Elasticsearch_HOST;
	public static String Elasticsearch_PORT;

	/**
	 * tableName
	 */
	public static String TABLE_NAME_pt_user_profile_mid;
	/**
	 * reconnect
	 */
	public static int ReconnectThreshold;

	static {

		ResourceBundle bundle = ResourceBundle.getBundle("application-ua");
		
		Flume_HOST =(String) bundle.getString("flume_host");
		Flume_PORT = (String) bundle.getString("flume_port");
		
		Kafka_HOST = (String) bundle.getString("kafka_host");
		Kafka_PORT = (String) bundle.getString("kafka_port");

		HBase_Zookeeper_QUORUM = (String) bundle.getString("haase_zookeeper_quorum");
		HBase_Zookeeper_QUORUM_HOST = (String) bundle.getString("haase_zookeeper_quorum_host");
		HBase_Zookeeper_CLIENTPORT = (String) bundle.getString("haase_zookeeper_clientport");
		HBase_ZOOKEEPER_PORT = (String) bundle.getString("haase_zookeeper_port");

		Cassandra_Presto_HOST = (String) bundle.getString("cassandra_presto_host");
		Cassandra_Presto_PORT = (String) bundle.getString("cassandra_presto_port");
		
		Presto_HOST = (String) bundle.getString("cassandra_presto_host");
				
		Elasticsearch_HOST = (String) bundle.getString("elasticsearch_host");
		Elasticsearch_PORT = (String) bundle.getString("elasticsearch_port");

		TABLE_NAME_pt_user_profile_mid = (String) bundle.getString("table_name_pt_user_profile_mid");

		ReconnectThreshold = Integer.parseInt((String) bundle.getString("reconnectthreshold"));

		Presto_Driver = (String) bundle.getString("presto_driver");

	}
	
	
	public static void main(String[] args) {
		System.out.println(Flume_HOST);
	}
}
