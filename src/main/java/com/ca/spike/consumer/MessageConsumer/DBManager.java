package com.ca.spike.consumer.MessageConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBManager {
	
	public void init() throws ClassNotFoundException, SQLException
	{
		
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

		 String url = "jdbc:sqlserver://SHAGA12\\SPIKE:1433;databaseName=spike";

		   String user = "TANT-A01\\shaga12";
		   String pass = "N0tall0wed";
		   Connection connection = DriverManager.getConnection(url, user, pass);
		/*String connectionString =  
                "jdbc:sqlserver://localhost:1433;"  
                + "database=spike;"  
                + "user=shaga12;"  
                + "password=N0tall0wed;"  
                + "encrypt=true;"  
                + "trustServerCertificate=false;"  
                + "hostNameInCertificate=*.database.windows.net;"  
                + "loginTimeout=30;"; */
	}

}
