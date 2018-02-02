package com.sdg.userprofile;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import net.sf.json.JSONObject;

public class App {
	public static void main(String[] args) throws Exception {
		new App().executeFromHBaseToElasticsearch();
	}
		
	public  void executeFromHBaseToElasticsearch() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set(ConfigHelper.HBase_Zookeeper_QUORUM, ConfigHelper.HBase_Zookeeper_QUORUM_HOST);
		conf.set(ConfigHelper.HBase_Zookeeper_CLIENTPORT, ConfigHelper.HBase_ZOOKEEPER_PORT);
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable table = null;
		ESPersistor esPersistor = new ESPersistor();
		esPersistor.initialize();
		boolean tableFlag = admin.tableExists(ConfigHelper.TABLE_NAME_pt_user_profile_mid);
		if (tableFlag) {
			table = new HTable(conf, ConfigHelper.TABLE_NAME_pt_user_profile_mid);
			Scan scan = new Scan();
			scan.setCaching(1000); //set the scan caching
			scan.setCacheBlocks(false);
			ResultScanner resultScanner = table.getScanner(scan);
			int n =1;
			while(true){
				Result result;
				long startTime = System.currentTimeMillis();
				int count = 0;
				while ((result = resultScanner.next()) != null) {
					long stopTime = System.currentTimeMillis();
					if (stopTime - startTime < 5000) {
						JSONObject jsonObj = convertResultToJson(result);
//						System.out.println(jsonObj.toString());
						esPersistor.persist(ConfigHelper.TABLE_NAME_pt_user_profile_mid+"_es", jsonObj.getString("pt_id"), jsonObj.toString());
						count++;						
					} else {
						System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date())+", the "+n+" count,the number is: " + count);
//						startTime = System.currentTimeMillis();
						break;
					}
				}
				n++;
				if(null == resultScanner.next()){
					System.out.println("Msg written done!");
					break;
				}
			}
			//esPersistor.close();
		} else {
			System.out.println("表不存在.... ");
			admin.close();
		}
	}
	
	private static JSONObject convertResultToJson(Result result) {
		if (result == null || result.isEmpty()) {
			return null;
		}
		JSONObject data = new JSONObject();
		for (KeyValue kv : result.list()) {
			data.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
		}

		return data;
	}

}
