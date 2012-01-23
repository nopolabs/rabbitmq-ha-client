package com.nopolabs.rabbitmq.ha.it;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.joshdevins.rabbitmq.client.ha.HaConnectionFactory;
import net.joshdevins.rabbitmq.client.ha.retry.AlwaysRetryStrategy;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.tools.Tracer;

public class IntegrationTest {
	
    private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

    private static final int TRACER_PORT = 5673;
	private static final String AMQP_HOST = ConnectionFactory.DEFAULT_HOST;
	private static final int AMQP_PORT = ConnectionFactory.DEFAULT_AMQP_PORT;
	
	private static final String TEST_EXCHANGE = "TEST-exchange";
	private static final String TEST_QUEUE = "TEST-queue";
	private static final String TEST_ROUTING_KEY = "TEST-routingKey";
	
	private TracerServer tracerServer;
	private HaConnectionFactory haFactory;
	private ConnectionFactory factory;
	private ExecutorService executor;
	
	@Test
	public void publisherTest() throws IOException {
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		for (int i = 0; i < 10000; i++) {
			String msg = "msg-" + i;
			channel.basicPublish(TEST_EXCHANGE, TEST_ROUTING_KEY, null, msg.getBytes());
			if (i == 9000) {
				tracerServer.breakConnection();
			}
		}
		
		channel.close();
		connection.close();
	}
	
	abstract class Client implements Runnable {
	    private Connection connection;
	    protected Channel channel;

	    Client() throws IOException {
            open();
	    }
	    
	    abstract void work() throws IOException;
	    
        public void run() {
            try {
                 work();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        private void open() throws IOException {
            connection = factory.newConnection();
            channel = connection.createChannel();
        }
        
        private void close() throws IOException {
            channel.close();
            connection.close();
        }
	    
	}
	
    abstract class TestPublisher extends Client {
        private String exchangeName;
        private String routingKey;
        
        TestPublisher(String exchangeName, String routingKey) throws IOException {
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
        }
        
        void send(byte[] msgBytes) throws IOException {
            channel.basicPublish(exchangeName, routingKey, null, msgBytes);
        }
    }
        
    abstract class TestConsumer extends Client {
         
        TestConsumer(String queueName) throws IOException {
            channel.basicConsume(queueName, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] msgBytes) throws IOException {
                    receive(msgBytes);
                }
            });
        }
        
        boolean receive(byte[] msgBytes) throws IOException {
            return true;
        }
    }
        
	@Before 
	public void setup() throws IOException, ClassNotFoundException {
	    
	    executor = Executors.newFixedThreadPool(10);
	    
		tracerServer = new TracerServer(TRACER_PORT, AMQP_HOST, AMQP_PORT);
		tracerServer.startup();
		
	    haFactory = new HaConnectionFactory();
	    haFactory.setRetryStrategy(new AlwaysRetryStrategy());
		factory = haFactory;
		factory.setHost("localhost");
		factory.setPort(TRACER_PORT);
		
		teardownQueue(TEST_EXCHANGE, TEST_QUEUE, TEST_ROUTING_KEY);
		setupQueue(TEST_EXCHANGE, TEST_QUEUE, TEST_ROUTING_KEY);
	}
	
	@After
	public void teardown() throws IOException, InterruptedException {
		Thread.sleep(500);
		tracerServer.shutdown();
//		teardownQueue(TEST_EXCHANGE, TEST_QUEUE, TEST_ROUTING_KEY);
		Thread.sleep(500);
	}
	
	private void setupQueue(String exchangeName, String queueName, String routingKey) throws IOException {
		ConnectionFactory connectionFactory = new ConnectionFactory();		
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();

		channel.exchangeDeclare(exchangeName, "direct", true);
		channel.queueDeclare(queueName, true, false, false, null);
		channel.queueBind(queueName, exchangeName, routingKey);
		
		channel.close();
		connection.close();
	}
	
	private void teardownQueue(String exchangeName, String queueName, String routingKey) throws IOException {
		ConnectionFactory connectionFactory = new ConnectionFactory();		
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();

		try {
			channel.queueUnbind(queueName, exchangeName, routingKey);
			channel.queueDelete(queueName);
			channel.exchangeDelete(exchangeName);
			channel.close();
		} catch (IOException e) {
			LOG.warn("error trying to teardown queue", e);
		}
		
		connection.close();
	}
	
	class TracerServer {
		
		private Constructor<Tracer> tracerConstructor;
		private int listenPort;
		private String connectHost;
		private int connectPort;
		private Thread thread;
		private Tracer.Logger logger;
		private Socket socket;
		
		TracerServer(int listenPort, String connectHost, int connectPort) throws ClassNotFoundException {
			this.listenPort = listenPort;
			this.connectHost = connectHost;
			this.connectPort = connectPort;
			Class<Tracer> cls = (Class<Tracer>)Class.forName("com.rabbitmq.tools.Tracer");
			Constructor<Tracer> ctors[] = (Constructor<Tracer>[])cls.getDeclaredConstructors();
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

		private void trace(Socket socket, String id) throws Exception {
			Tracer tracer = tracerConstructor.newInstance(socket, id, connectHost, connectPort, logger);	
			Thread thread = new Thread(tracer);
			thread.start();
            LOG.info(id + " started");
		}
		
		void breakConnection() {
			if (socket != null) {
				try {
					socket.close();
					socket = null;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		void startup() {
	    	logger = new Tracer.AsyncLogger(System.out);
	    	logger.start();
	        thread = new Thread() {
	        	@Override
	        	public void run() {
	    	        try {
	    	            ServerSocket server = new ServerSocket(listenPort);
	    	            int counter = 0;
	    	            while (true) {
	    	            	socket = server.accept();
	    	            	String id = "Tracer-" + (counter++);
	    	                trace(socket, id);
	    	            }
	    	        } catch (Exception e) {
	    	            e.printStackTrace();
	    	        }	        		
	        	}
	        };
			thread.start();
		}
		
		void shutdown() {
            logger.stop();
			thread.interrupt();
		}
	}
	
}
