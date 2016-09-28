package com.ca.spike.consumer.threading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.ca.spike.consumer.db.MYSQL57DBManager;

public class EventDataInsertExecutor {
	private static final int POOL_SIZE = 5;
	public ThreadPoolExecutor executor = null;
	private int counter = 1;

	public EventDataInsertExecutor() {
		super();

		ThreadFactory threadFactory = new ThreadFactory() {
			public Thread newThread(Runnable task) {
				EventDataInsertWorkerThread thread = new EventDataInsertWorkerThread(task,
						"EventDataInsertWorkerThread - " + counter++);
				thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

					@Override
					public void uncaughtException(Thread exceptionedThread, Throwable error) {
						System.out.println("Error EventDataInsertWorkerThread.uncaughtExceptionHandler" + error);
						try {
							EventDataInsertWorkerThread thread = (EventDataInsertWorkerThread) exceptionedThread;
							thread.preparedStatement.get().close();
							thread.localConnection.get().close();
						} catch (SQLException e) {
							System.out.println(
									"Error EventDataInsertWorkerThread.uncaughtExceptionHandler-->Catch SQLException"
											+ e);
							e.printStackTrace();
						}

					}
				});
				return thread;
			}
		};
		LinkedBlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<Runnable>(10000);

		this.executor = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 500L, TimeUnit.MILLISECONDS, blockingQueue,
				threadFactory);

	}

	public EventDataInsertExecutor(ThreadPoolExecutor executor) {
		this.executor = executor;
	}

	public void execute(EventInsertTask eventInsertTask) {
		try {
			executor.execute(eventInsertTask);
		} catch (Exception e) {
			System.out.println(executor.getActiveCount());
			System.out.println(executor.getPoolSize());
			System.out.println(executor.getLargestPoolSize());
			e.printStackTrace();

			System.exit(0);
		}
	}

	public class EventDataInsertWorkerThread extends Thread {
		private HashMap<String, Integer> map = new HashMap<String, Integer>();
		private String sql = new String(
				"insert into spike (EVENT_HEADER, `Target host`, `Event type`, Status, Class, Resource, "
						+ "Access, `User name`, Terminal, Program, Date, Time, Details, `User Logon Session ID`, "
						+ "`Audit flags`,`Effective user name`, nStatus,`Time Stamp`, nReason, nStage) "
						+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

		ThreadLocal<Connection> localConnection = new ThreadLocal<Connection>() {
			@Override
			protected Connection initialValue() {
				Connection connection = MYSQL57DBManager.getConnection();
				try {
					connection.setAutoCommit(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return connection;
			}

		};

		ThreadLocal<PreparedStatement> preparedStatement = new ThreadLocal<PreparedStatement>() {
			@Override
			protected PreparedStatement initialValue() {
				try {
					return localConnection.get().prepareStatement(sql);
				} catch (SQLException e) {
					System.out.println("EventDataInsertWorkerThread preparedStatement thread local " + e);
					e.printStackTrace();
				}
				return null;
			}

		};
		ThreadLocal<Integer> batchSize = new ThreadLocal<Integer>() {
			@Override
			protected Integer initialValue() {
				return 0;
			}
		};

		public EventDataInsertWorkerThread(Runnable task, String name) {
			super(task, name);
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

		public HashMap<String, Integer> getMap() {
			return map;
		}
	}
}