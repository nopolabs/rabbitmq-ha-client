package com.nopolabs.rabbitmq.ha.it;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.joshdevins.rabbitmq.client.ha.HaConnectionFactory;
import net.joshdevins.rabbitmq.client.ha.retry.AlwaysRetryStrategy;
import net.joshdevins.rabbitmq.client.ha.retry.SimpleRetryStrategy;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.tools.Tracer;

public class IntegrationTest {
	
    private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

    private static final int FIRST_PROXY_PORT = 5673;
    private static final int LAST_PROXY_PORT = FIRST_PROXY_PORT + 4;
    
	private static final String AMQP_HOST = ConnectionFactory.DEFAULT_HOST;
	private static final int AMQP_PORT = ConnectionFactory.DEFAULT_AMQP_PORT;
	
	private static final String TEST_EXCHANGE = "TEST-exchange";
	private static final String TEST_QUEUE = "TEST-queue";
	private static final String TEST_ROUTING_KEY = "TEST-routingKey";
	
    private TracerProxy[] proxies;
    private Address[] proxyAddresses;
	
	private ConnectionFactory factory;
	
	private ExecutorService executor;	
    
    @Before 
    public void setup() throws IOException, ClassNotFoundException {
        
        executor = Executors.newFixedThreadPool(10);
        
        List<TracerProxy> proxyList = new LinkedList<TracerProxy>();
        List<Address> addressList = new LinkedList<Address>();
        
        for (int port = FIRST_PROXY_PORT; port <= LAST_PROXY_PORT; port++) {
            proxyList.add(new TracerProxy("proxy-" + port, port, AMQP_HOST, AMQP_PORT));
            addressList.add(new Address("localhost", port));
        }
        proxies = proxyList.toArray(new TracerProxy[0]);
        proxyAddresses = addressList.toArray(new Address[0]);
        
        for (TracerProxy proxy : proxyList) {
            executor.execute(proxy);
        }
        
        factory = createConnectionFactory(proxyAddresses);
        
        teardownQueue(TEST_EXCHANGE, TEST_QUEUE, TEST_ROUTING_KEY);
        setupQueue(TEST_EXCHANGE, TEST_QUEUE, TEST_ROUTING_KEY);
    }
    
    ConnectionFactory createConnectionFactory(Address[] addresses) {
        HaConnectionFactory factory = new HaConnectionFactory();
        factory.setHost(addresses[0].getHost());
        factory.setPort(addresses[0].getPort());
        factory.setRetryStrategy(new SimpleRetryStrategy());
        return factory;
    }
    
    @After
    public void teardown() throws IOException, InterruptedException {
        Thread.sleep(500);
    }
    
    Connection newHaConnection() throws IOException {
        return factory.newConnection(proxyAddresses);
    }
    
	@Test
	public void publisherTest() throws IOException {
		
		PublishingClient p = new PublishingClient(newHaConnection(), TEST_EXCHANGE, TEST_ROUTING_KEY) {
			@Override
			void work() throws IOException {
				for (int i = 0; i < 10; i++) {
					send(("msg-" + i).getBytes());
					try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
					if ((i % 2) == 0) {
					    System.out.println(i + " sent");
					    proxies[i / 2].shutdown();
					}
				}
			}			
		};
		
		executor.execute(p);
		
        while (!executor.isTerminated()) {} // wait for all jobs to complete
	}
	
	@Ignore @Test
	public void consumerTest() throws IOException {
		
		ConsumingClient c = new ConsumingClient(newHaConnection(), TEST_QUEUE) {
			
			List<String> rcvd = Collections.synchronizedList(new ArrayList<String>());
			
			@Override
			void work() throws IOException {
				while (rcvd.size() < 1000) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
				}					
			}
			
	        boolean receive(byte[] msgBytes) throws IOException {
	        	rcvd.add(new String(msgBytes));
	            return true;
	        }
		};
		
		executor.execute(c);
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
	
	class TracerProxy implements Runnable {
		
		private String id;
		private int listenPort;
		private String connectHost;
		private int connectPort;
		private Constructor<Tracer> tracerConstructor;
		private Tracer.Logger logger;
		private ServerSocket serverSocket;
		private Socket socket;
		
		TracerProxy(String id, int listenPort, String connectHost, int connectPort) throws ClassNotFoundException {
			this.id = id;
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
		
		String getHost() {
		    return connectHost;
		}
		
		int getPort() {
		    return connectPort;
		}

		public void run() {
			logger = new Tracer.AsyncLogger(System.out);
			try {
			    serverSocket = new ServerSocket(listenPort);
				int counter = 0;
				while (true) {
					counter++;
					String tid = id + "-" + counter;
                    System.out.println("Socket accept() " + tid);
                    socket = serverSocket.accept();
                    System.out.println("Creating tracer " + tid);
                    Tracer tracer = tracerConstructor.newInstance(socket, tid, connectHost, connectPort, logger);
                    System.out.println("Starting tracer " + tid);
                    tracer.start();
                    System.out.println("Started tracer " + tid);
                }
            } catch (Exception e) {
                 logger.stop(); // will stop shared logger thread
                 e.printStackTrace();
            }					
		}
		
		void shutdown() {
		    System.out.println("CLOSE: " + this.id);
			logger.stop();
			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	abstract class Client implements Runnable {
		
	    private Connection connection;
	    protected Channel channel;

	    Client(Connection connection) throws IOException {
            open(connection);
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
        
        private void open(Connection connection) throws IOException {
            this.connection = connection;
            this.channel = connection.createChannel();
        }
        
        private void close() throws IOException {
            channel.close();
            connection.close();
        }
	    
	}
	
    abstract class PublishingClient extends Client {
        private String exchangeName;
        private String routingKey;
        
        PublishingClient(Connection connection, String exchangeName, String routingKey) throws IOException {
        	super(connection);
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
        }
        
        void send(byte[] msgBytes) throws IOException {
            channel.basicPublish(exchangeName, routingKey, null, msgBytes);
        }
    }
        
    abstract class ConsumingClient extends Client {
         
    	ConsumingClient(Connection connection, String queueName) throws IOException {
        	super(connection);
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
        
}
