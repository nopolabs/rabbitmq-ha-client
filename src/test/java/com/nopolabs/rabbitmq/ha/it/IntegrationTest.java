package com.nopolabs.rabbitmq.ha.it;

import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.net.Socket;

import net.joshdevins.rabbitmq.client.ha.HaConnectionFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import Tracer.AsyncLogger;
import Tracer.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.tools.Tracer;

public class IntegrationTest {
	
	private static final int TRACER_PORT = 5673;
	private static final String RABBIT_HOST = "localhost";
	private static final int RABBIT_PORT = 5672;
	
	private static final String TEST_EXCHANGE = "TEST-exchange";
	private static final String TEST_QUEUE = "TEST-queue";
	private static final String TEST_ROUTING_KEY = "TEST-routingKey";
	
	private TracerThreadFactory tracerThreadFactory;
	private Thread tracerThread;
	private HaConnectionFactory haFactory;
	private ConnectionFactory factory;
	
	@Test
	public void publisherTest() {
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		declareAndBindExchangeAndQueue(channel, TEST_EXCHANGE, TEST_QUEUE, TEST_ROUTING_KEY);
		
		byte[] messageBodyBytes = "Hello, world!".getBytes();
		channel.basicPublish(TEST_EXCHANGE, TEST_ROUTING_KEY, null, messageBodyBytes);
		
		channel.close();
		connection.close();
	}
	
	@Test
	public void consumerTest() {
		
	}
	
	@Test
	public void publisherClusterTest() {
		factory.newConnection();
	}
	
	@Test
	public void consumerClusterTest() {
		
	}
	
	@Before 
	public void setUp() {
		tracerThreadFactory = new TracerThreadFactory();
		tracerThread = tracerThreadFactory.createTracerThread(TRACER_PORT, RABBIT_HOST, RABBIT_PORT);
		tracerThread.start();
		
	    haFactory = new HaConnectionFactory();
		factory = haFactory;
		factory.setHost(RABBIT_HOST);
		factory.setPort(RABBIT_PORT);
	}
	
	@After
	public void tearDown() {
		tracerThread.interrupt();
	}
	
	private void declareAndBindExchangeAndQueue(Channel channel, String exchangeName, String queueName, String routingKey) {
		channel.exchangeDeclare(exchangeName, "direct", true);
		channel.queueDeclare(queueName, true, false, false, null);
		channel.queueBind(queueName, exchangeName, routingKey);
	}
	
	class TracerThreadFactory {
		
		private Constructor<Tracer> tracerConstructor;
		
		TracerThreadFactory() {
			Class<Tracer> cls = (Class<Tracer>)Class.forName("org.rabbitmq.tools.Tracer");
			Constructor<Tracer> ctors[] = (Constructor<Tracer>)cls.getDeclaredConstructors();
			for (Constructor<Tracer> ctor : ctors) {
				Class pTypes[] = ctor.getParameterTypes();
				if (pTypes.length == 5
						&& pTypes[0].equals(Socket.class)
						&& pTypes[1].equals(String.class)
						&& pTypes[2].equals(String.class)
						&& pTypes[3].equals(int.class)
						&& pTypes[4].equals(Tracer.Logger.class)) {
					ctor.setAccessible(true);
					tracerConstructor = ctor;
					return;
				}
			}
			throw new RuntimeException("Could not find constructor: Tracer(Socket sock, String id, String host, int port, Logger logger)");
		}
		
	    Thread createTracerThread(final int listenPort, final String connectHost, final int connectPort) {
			
	        return new Thread() {
	        	
	        	private Tracer.Logger logger = new Tracer.AsyncLogger(System.out); // initially stopped
	        	private Thread tracerThread;

	        	@Override
	        	public void run() {
	        		
	        		logger.start();
	    	        try {
	    	            ServerSocket server = new ServerSocket(listenPort);
	    	            int counter = 0;
	    	            while (true) {
	    	                Socket conn = server.accept();
	    	                Tracer tracer = tracerConstructor.newInstance(
	    	                		conn
	    	                          , "Tracer-" + (counter++)
	    	                          , connectHost, connectPort
	    	                          , logger
	    	                          );
	    	                tracerThread = new Thread(tracer);
	    	                tracerThread.start();
	    	            }
	    	        } catch (Exception e) {
	    	            logger.stop(); // will stop shared logger thread
	    	            e.printStackTrace();
	    	        }
	        		
	        	}
	        	
	        	@Override
	        	public void interrupt() {
	        		tracerThread.interrupt();
	        		logger.stop(); // will stop shared logger thread
	        		super.interrupt();
	        	}
	        };
		}
	}
	

}
