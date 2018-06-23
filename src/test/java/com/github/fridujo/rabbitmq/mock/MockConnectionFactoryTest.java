package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.compatibility.MockConnectionFactoryWithoutAddressResolver;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class MockConnectionFactoryTest {

    @Test
    void configure_by_params() throws IOException, TimeoutException {
        ConnectionFactory factory = new MockConnectionFactory();

        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection connection = factory.newConnection();

        assertThat(connection).isInstanceOf(MockConnection.class);
    }

    @Test
    void configure_by_uri() throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        ConnectionFactory factory = new MockConnectionFactory();

        factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");

        Connection connection = factory.newConnection();

        assertThat(connection).isInstanceOf(MockConnection.class);
    }

    @Test
    void use_alternate_factory() throws IOException, TimeoutException {
        ConnectionFactory factory = new MockConnectionFactoryWithoutAddressResolver();

        Connection connection = factory.newConnection(null, (List<Address>) null, null);

        assertThat(connection).isInstanceOf(MockConnection.class);
    }
}
