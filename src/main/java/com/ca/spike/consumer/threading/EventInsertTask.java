package com.ca.spike.consumer.threading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.ca.spike.consumer.threading.EventDataInsertExecutor.EventDataInsertWorkerThread;

public class EventInsertTask implements Runnable {
	public EventInsertTask(String content) {
		this.content = content;
	}

	public void run() {
		long time=System.currentTimeMillis();

		try {
			int startIndex = 0;
			int endIndex = 0;
			int length = content.length();
			String keyValue = null;
			Connection connection = null;
			connection = ((EventDataInsertWorkerThread) Thread.currentThread()).localConnection.get();
			Map<String, Integer> map = ((EventDataInsertWorkerThread) Thread.currentThread()).getMap();
			PreparedStatement preparedStatement = ((EventDataInsertWorkerThread) Thread
					.currentThread()).preparedStatement.get();
			while (startIndex < length) {
				endIndex = content.indexOf(NEWLINE, startIndex);
				endIndex = endIndex > 0 ? endIndex : length;
				keyValue = content.substring(startIndex, endIndex);
				startIndex = endIndex + 1;
				int colonIndex = keyValue.indexOf(":");
				if (colonIndex < 0) {
					if ("EVENT_END".equals(keyValue.trim())) {
						preparedStatement.addBatch();
					}
					continue;
				}
				String field = keyValue.substring(0, colonIndex);
				String value = keyValue.substring(colonIndex + 1, keyValue.length());

				if (map.containsKey(field))
					preparedStatement.setString(map.get(field), value);
			}
			preparedStatement.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally{
			System.out.println(System.currentTimeMillis()-time);
		}
	}

	private String content = null;
	private static final String NEWLINE = "\n";
}
