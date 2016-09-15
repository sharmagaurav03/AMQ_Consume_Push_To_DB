package com.ca.spike.consumer.db;

import java.sql.*;
import org.apache.commons.dbcp2.*;

public class MYSQL57DBManager {
	private static volatile BasicDataSource connectionPool = null;
	static {
		try {
			try {
				Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
				String connectionUrl =("jdbc:mysql://shaga12-I180958:3306/spike?user=test&password=*0D3CED9BEC10A777AEC23CCC353A8C08A633045E#%25@abcd&autoReconnect=true&useSSL=false");
				connectionPool = new BasicDataSource();
				connectionPool.setDriverClassName("com.mysql.cj.jdbc.Driver");
				connectionPool.setUrl(connectionUrl);
				connectionPool.setInitialSize(20);
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
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
