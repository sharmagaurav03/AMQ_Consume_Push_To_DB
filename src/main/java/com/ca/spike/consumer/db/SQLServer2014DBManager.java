package com.ca.spike.consumer.db;

import java.sql.*;
import org.apache.commons.dbcp2.*;

public class SQLServer2014DBManager {
	private static volatile BasicDataSource connectionPool = null;
	static {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		String connectionUrl = "jdbc:sqlserver://SHAGA12\\SPIKE:1433;database=SPIKE;user=test;password=N0tall0wed;packetSize=4096;";
		connectionPool = new BasicDataSource();
		connectionPool.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		connectionPool.setUrl(connectionUrl);
		connectionPool.setInitialSize(20);
	}

	public static Connection getConnection() {
		try {
			return connectionPool.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

}
