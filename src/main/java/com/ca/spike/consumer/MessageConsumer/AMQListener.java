package com.ca.spike.consumer.MessageConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

public class AMQListener implements MessageListener {
	private static final String NEWLINE = "\n";
	static AtomicLong totalTime = new AtomicLong(0);
	private StringBuffer sql;
	private Connection connection;
	private PreparedStatement preparedStatement;
	private Executor executor = Executors.newFixedThreadPool(5);
	long startTime;

	public AMQListener() {
		sql = new StringBuffer(
				"insert into dbo.spike (EVENT_HEADER, \"Target host\", \"Event type\", Status, Class, Resource, "
						+ "Access, \"User name\", Terminal, Program, Date, Time, Details, \"User Logon Session ID\", "
						+ "\"Audit flags\",\"Effective user name\", nStatus,\"Time Stamp\", nReason, nStage) "
						+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		this.connection = getConnection();
		try {
			preparedStatement = connection.prepareStatement(this.sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void onMessage(Message message) {
		BytesMessage byteMessage = (BytesMessage) message;
		try {
			byte[] messageData = new byte[(int) byteMessage.getBodyLength()];
			byteMessage.readBytes(messageData);
			String textMessage = new String(messageData, "UTF-8");
			saveMessage(textMessage);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void saveMessage(String eventMessages) throws SQLException {
		executor.execute(new EventManager(this.preparedStatement, this.connection, eventMessages));
	}

	private class EventManager implements Runnable {
		private PreparedStatement preparedStatement;
		private Connection connection;
		private String content;
		private HashMap<String, Integer> map = new HashMap<String, Integer>();

		EventManager(PreparedStatement preparedStatement, Connection connection, String content) {
			this.preparedStatement = preparedStatement;
			this.connection = connection;
			this.content = content;

			map.put("EVENT_HEADER", 1);
			map.put("Target host", 2);
			map.put("Event type", 3);
			map.put("Status", 4);
			map.put("Class", 5);
			map.put("Resource", 6);
			map.put("Access", 7);
			map.put("User name", 8);
			map.put("Terminal", 9);
			map.put("Program", 10);
			map.put("Date", 11);
			map.put("Time", 12);
			map.put("Details", 13);
			map.put("User Logon Session ID", 14);
			map.put("Audit flags", 15);
			map.put("Effective user name", 16);
			map.put("nStatus", 17);
			map.put("Time Stamp", 18);
			map.put("nReason", 19);
			map.put("nStage", 20);
		}

		public void run() {
			int startIndex = 0;
			int endIndex = 0;
			int length = content.length();
			String keyValue = null;
			while (startIndex < length) {
				endIndex = content.indexOf(NEWLINE, startIndex);
				endIndex = endIndex > 0 ? endIndex : length;
				keyValue = content.substring(startIndex, endIndex);
				startIndex = endIndex + 1;
				int colonIndex = keyValue.indexOf(":");
				if (colonIndex < 0) {
					if ("EVENT_END".equals(keyValue.trim())) {
						try {
							preparedStatement.addBatch();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					continue;
				}
				String field = keyValue.substring(0, colonIndex);
				String value = keyValue.substring(colonIndex + 1, keyValue.length());

				if (map.containsKey(field))
					try {
						preparedStatement.setString(map.get(field), value);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}

			try {
				preparedStatement.executeBatch();
//				connection.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private Connection getConnection() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		String connectionUrl = "jdbc:sqlserver://SHAGA12\\SPIKE:1433;database=SPIKE;user=test;password=N0tall0wed;packetSize=4096;";
		try {
			Connection con = DriverManager.getConnection(connectionUrl);
			con.setAutoCommit(true);
			return con;
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return null;

	}

}
