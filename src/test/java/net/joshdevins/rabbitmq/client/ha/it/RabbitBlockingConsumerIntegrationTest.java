package net.joshdevins.rabbitmq.client.ha.it;

import net.joshdevins.rabbitmq.client.ha.HaConnectionFactory;
import net.joshdevins.rabbitmq.client.ha.HaConnectionListener;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.rabbitmq.client.AMQP.Queue.BindOk;
import com.rabbitmq.client.Address;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/META-INF/spring/applicationContext.xml")
public class RabbitBlockingConsumerIntegrationTest {

    public static class PojoHandler {

        private int msgCount;

        public void handleMessage(final byte[] bytes) {

            synchronized (this) {
                ++msgCount;
            }

            LOG.info("Thread [" + Thread.currentThread().getId() + "] message: n=" + msgCount + ", body="
                    + new String(bytes));
        }
    }

    private static final Logger LOG = Logger.getLogger(RabbitBlockingConsumerIntegrationTest.class);

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private HaConnectionFactory haConnectionFactory;

    @Autowired
    private RabbitTemplate template;

    @Before
    public void before() {
        // add my connection listener to the HaConnectionFactory
        Address[] addresses = new Address[] { new Address("localhost") };
        HaConnectionListener listener = new TestHaConnectionListener(haConnectionFactory, addresses);
        haConnectionFactory.addHaConnectionListener(listener);
    }

    @Test
    public void testAsyncConsume() throws InterruptedException {
        LOG.warn("testAsyncConsume enter");

        BindOk bindOk = template.execute(new TestChannelCallback());
        Assert.assertNotNull(bindOk);

        // setup async container
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames("testQueue");
        container.setConcurrentConsumers(5);
        // container.setChannelTransacted(true);

        MessageListenerAdapter adapter = new MessageListenerAdapter();
        adapter.setDelegate(new PojoHandler());
        container.setMessageListener(adapter);
        container.afterPropertiesSet();
        container.start();

        while (true) {
            LOG.warn("testAsyncConsume sleeping");
            Thread.sleep(10000);
        }

        // container.stop();
        // container.shutdown();
    }
}
