package com.ca.spike.consumer.MessageConsumer;

import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws JMSException, InterruptedException {
		CountDownLatch startLatch = new CountDownLatch(1);
		Thread t1 = new Thread(new Consumer(startLatch),"1");
//		Thread t2 = new Thread(new Consumer(null, null, startLatch),"2");
//		Thread t3 = new Thread(new Consumer(null, null, startLatch),"3");
//		Thread t4 = new Thread(new Consumer(null, null, startLatch),"4");
//		Thread t5 = new Thread(new Consumer(null, null, startLatch),"5");
//		Thread t6 = new Thread(new Consumer(connection, queueName, startLatch),"6");
//		Thread t7 = new Thread(new Consumer(connection, queueName, startLatch),"7");
//		Thread t8 = new Thread(new Consumer(connection, queueName, startLatch),"8");
//		Thread t9 = new Thread(new Consumer(connection, queueName, startLatch),"9");
//		Thread t10 = new Thread(new Consumer(connection, queueName, startLatch),"10");
//		Thread t11 = new Thread(new Consumer(connection, queueName, startLatch),"11");
//		Thread t12 = new Thread(new Consumer(connection, queueName, startLatch),"12");
//		Thread t13 = new Thread(new Consumer(connection, queueName, startLatch),"13");
//		Thread t14 = new Thread(new Consumer(connection, queueName, startLatch),"14");
//		Thread t15 = new Thread(new Consumer(connection, queueName, startLatch),"15");
//		Thread t16 = new Thread(new Consumer(connection, queueName, startLatch),"16");
//		Thread t17 = new Thread(new Consumer(connection, queueName, startLatch),"17");
//		Thread t18 = new Thread(new Consumer(connection, queueName, startLatch),"18");
//		Thread t19 = new Thread(new Consumer(connection, queueName, startLatch),"19");
//		Thread t20 = new Thread(new Consumer(connection, queueName, startLatch),"20");
		t1.start();
//		t2.start();
//		t3.start();
//		t4.start();
//		t5.start();
//		t6.start();
//		t7.start();
//		t8.start();
//		t9.start();
//		t10.start();
//		t11.start();
//		t12.start();
//		t13.start();
//		t14.start();
//		t15.start();
//		t16.start();
//		t17.start();
//		t18.start();
//		t19.start();
//		t20.start();
		Thread.sleep(1000);
		startLatch.countDown();
	}

	static class Consumer implements Runnable {
		private CountDownLatch startLatch;

		Consumer(CountDownLatch startLatch) {
			this.startLatch=startLatch;
		}

		public void run() {
			try {
				ActiveMQConnectionFactory connectionFactory=null;
				Connection connection=null;
				Session session = null;
				String uri = "nio://SHAGA12:61616";
				String queueName = "firstQueue?socketBufferSize=131072&ioBufferSize=16384&consumer.prefetchSize=20";
				connectionFactory = new ActiveMQConnectionFactory(uri);
				connectionFactory.isDispatchAsync();
				connectionFactory.setAlwaysSessionAsync(true);
				connection = connectionFactory.createConnection();
				connection.start();
				session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				 Queue queue = session.createQueue(queueName);
				 MessageConsumer consumer=session.createConsumer(queue);
				 AMQListener amqListener=new AMQListener();
				 try {
					 
					startLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 consumer.setMessageListener(amqListener);
			} catch (JMSException e) {
				e.printStackTrace();
			} 

		}

	}
}
