package com.ca.spike.consumer.MessageConsumer;

import java.sql.SQLException;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

import com.ca.spike.consumer.threading.EventDataInsertExecutor;
import com.ca.spike.consumer.threading.EventInsertTask;

public class AMQListener implements MessageListener {
	
	private EventDataInsertExecutor eventDataInsertExecutor = new EventDataInsertExecutor();

	public void onMessage(Message message) {
		BytesMessage byteMessage = (BytesMessage) message;
		try {
			byte[] messageData = new byte[(int) byteMessage.getBodyLength()];
			byteMessage.readBytes(messageData );
			String textMessage = new String(messageData, "UTF-8");
			messageData=null;
			saveMessage(textMessage);
		} catch (Exception e) {
			System.out.println("Error: EventDataInsertExecutor.onMessage");
			e.printStackTrace();
		}

	}

	public void saveMessage(String eventMessages) throws SQLException {
		eventDataInsertExecutor.execute(new EventInsertTask(eventMessages));
	}
}
