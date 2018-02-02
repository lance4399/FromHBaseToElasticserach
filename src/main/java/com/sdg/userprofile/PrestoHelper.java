package com.sdg.userprofile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import com.facebook.presto.jdbc.PrestoResultSet;


public class PrestoHelper  {
	private Set<String> uuids;
	
	public PrestoHelper(Set<String> uuids) {
		this.uuids = uuids;
	}
 
	 public static void main(String[] args) throws Exception {
	 String set = null; //1a8d3e9dba6442689a42c3562b25e7af
	 
	 }

	public static void execute() throws SQLException  {
		String pt_id = "ffdacai38";
			Connection connection = null;
			Statement stmt = null ;
			try {
				Class.forName(ConfigHelper.Presto_Driver);
				connection = DriverManager.getConnection("jdbc:presto://" + ConfigHelper.Cassandra_Presto_HOST + ":"
						+ ConfigHelper.Cassandra_Presto_PORT + "/hive/dw", ConfigHelper.TABLE_NAME_pt_user_profile_mid, null);
				stmt = connection.createStatement();
				String sql = "select * from " + ConfigHelper.TABLE_NAME_pt_user_profile_mid + " where pt_id='" + pt_id + "'";
				PrestoResultSet rs = (PrestoResultSet) stmt.executeQuery(sql);
	
				if (!rs.next()) {
//					result_uuids.add(uuid);
				}
				stmt.close();
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				stmt.close();
				connection.close();
			}
	}
	
}
