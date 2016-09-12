package com.ca.spike.consumer.MessageConsumer;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class AMQListener implements MessageListener {
	private static final String NEWLINE = "\n";
	static AtomicLong totalTime = new AtomicLong(0);
	int start = 0;
	long startTime;
	HashMap<String, Integer> map = new HashMap<String, Integer>();
	private String sql;
	private Connection connection;
	private PreparedStatement preparedStatement;
	private int batchSize;

	public AMQListener() {
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

		sql = "insert into dbo.spike (EVENT_HEADER, \"Target host\", \"Event type\", Status, Class, Resource, "
				+ "Access, \"User name\", Terminal, Program, Date, Time, Details, \"User Logon Session ID\", "
				+ "\"Audit flags\",\"Effective user name\", nStatus,\"Time Stamp\", nReason, nStage) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		this.connection = getConnection();
		try {
			preparedStatement = connection.prepareStatement(this.sql );
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void onMessage(Message message) {
		if (start++ == 0)
			startTime = System.currentTimeMillis();
		BytesMessage byteMessage = (BytesMessage) message;
		try {
			byte[] messageData = new byte[(int) byteMessage.getBodyLength()];
			byteMessage.readBytes(messageData);
			String textMessage = new String(messageData, "UTF-8");
			saveMessage(textMessage);

			/*if (start % 600 == 0) {
				// System.out.println(Thread.currentThread().getName() + " " +
				// (System.currentTimeMillis() - startTime));
				System.out.println(System.currentTimeMillis()-startTime);
				startTime = System.currentTimeMillis();
			}
*/
		} catch (JMSException | UnsupportedEncodingException | SQLException e) {
			e.printStackTrace();
		}

	}

	public int saveMessage(String eventMessages) throws SQLException {
		return usingWithoutSplitAtAll(eventMessages);
	}
	
	private int usingWithoutSplitAtAll(String content) throws SQLException {

			int startIndex = 0;
			int endIndex = 0;
			int length = content.length();
			String keyValue = null;
			PreparedStatement preparedStatement = getPreparedStatement();
			
			while (startIndex < length) {
				endIndex = content.indexOf(NEWLINE, startIndex);
				endIndex = endIndex > 0 ? endIndex : length;
				keyValue = content.substring(startIndex, endIndex);
				startIndex = endIndex + 1;
				int colonIndex = keyValue.indexOf(":");
				if (colonIndex < 0) {
					if ("EVENT_END".equals(keyValue.trim())) {
						preparedStatement.addBatch();
						++batchSize;
					}
					continue;
				}
				String field = keyValue.substring(0, colonIndex);
				String value = keyValue.substring(colonIndex + 1, keyValue.length());
				
				if (map.containsKey(field))
					preparedStatement.setString(map.get(field), value);
			}
			int resultset = 0;
			if (batchSize % 900 == 0)
			{
				int[] resultsets = preparedStatement.executeBatch();
//				for(int k = 0; k<resultsets.length;k++)
//				{
//					System.out.print(resultsets[k]+", ");
//				}
				this.connection.commit();
			}
			return resultset;
	}

	private int parseWithoutUsingSplit(String eventMessages) throws SQLException {

		long startTime = System.currentTimeMillis();
		PreparedStatement preparedStatement = getPreparedStatement();
		String[] keyValue = eventMessages.split("\n");

		for (int j = 0; j < keyValue.length; j++) {
			if (keyValue[j].trim().length() == 0)
				continue;
			if ("EVENT_END".equals(keyValue[j])) {
				preparedStatement.addBatch();
				batchSize++;
				continue;
			}
			int colonIndex = keyValue[j].indexOf(":");

			String field = keyValue[j].substring(0, colonIndex);
			String value = keyValue[j].substring(colonIndex + 1, keyValue[j].length());
			if (map.containsKey(field))
				preparedStatement.setString(map.get(field), value);
		}

		// }
		int resultset = 0;
		if (batchSize % 900 == 0)
		{
			resultset = preparedStatement.executeUpdate();
//			getConnection().commit();
		}

		System.out.println(totalTime.addAndGet(System.currentTimeMillis() - startTime));
		return resultset;

	}
	

	private int[] parseUsingSplit(String eventMessages) throws SQLException {
		long startTime = System.currentTimeMillis();
		PreparedStatement preparedStatement = getPreparedStatement();
		String[] eventMessagesArray = eventMessages.split("EVENT_END\n");

		for (String eventMessage : eventMessagesArray) {

			if (eventMessage.trim().length() == 0)
				continue;

			String[] keyValue = eventMessage.split("\n");
			for (int j = 0; j < keyValue.length; j++) {
				int colonIndex = keyValue[j].indexOf(":");
				String field = null;
				field = keyValue[j].substring(0, colonIndex);
				String value = keyValue[j].substring(colonIndex + 1, keyValue[j].length());
				if (map.containsKey(field))
					preparedStatement.setString(map.get(field), value);
			}
			preparedStatement.addBatch();

		}
		int[] resultset = preparedStatement.executeBatch();

		System.out.println(totalTime.addAndGet(System.currentTimeMillis() - startTime));
		return resultset;
	}

	private PreparedStatement getPreparedStatement() throws SQLException {
		return this.preparedStatement;
	}

	private Connection getConnection() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		String connectionUrl = "jdbc:sqlserver://SHAGA12\\SPIKE:1433;database=SPIKE;user=test;password=N0tall0wed;packetSize=4096;sendStringParametersAsUnicode=false;";
		try {
			Connection con = DriverManager.getConnection(connectionUrl);
			con.setAutoCommit(false);
//			con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			return con;
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return null;

	}

}