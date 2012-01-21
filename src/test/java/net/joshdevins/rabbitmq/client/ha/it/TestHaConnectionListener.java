package net.joshdevins.rabbitmq.client.ha.it;

import java.util.HashMap;

import org.apache.log4j.Logger;

import net.joshdevins.rabbitmq.client.ha.AbstractHaConnectionListener;
import net.joshdevins.rabbitmq.client.ha.HaConnectionProxy;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TestHaConnectionListener extends AbstractHaConnectionListener {

    private static final Logger LOG = Logger.getLogger(RabbitBlockingConsumerIntegrationTest.class);

    private final ConnectionFactory connectionFactory;

    private final Address[] addresses;

    public TestHaConnectionListener(final ConnectionFactory connectionFactory, final Address[] addresses) {

        this.connectionFactory = connectionFactory;
        this.addresses = addresses;
    }

    @Override
    public void onReconnection(final HaConnectionProxy connectionProxy) {

        LOG.warn("onReconnection");
        
        // use a separate connection and channel to avoid race conditions with any operations that are blocked
        // waiting for the reconnection to finish...and don't cache anything otherwise you get the same problem!
        try {
            Connection connection = connectionFactory.newConnection(addresses);
            Channel channel = connection.createChannel();

            channel.queueDeclare("testQueue", true, true, true, new HashMap<String,Object>());
            channel.queueBind("testQueue", "amq.topic", "#");

            channel.close();
            connection.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
