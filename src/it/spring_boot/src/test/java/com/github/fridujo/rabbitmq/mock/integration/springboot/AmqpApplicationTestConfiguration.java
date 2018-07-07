package com.github.fridujo.rabbitmq.mock.integration.springboot;

import com.github.fridujo.rabbitmq.mock.compatibility.MockConnectionFactoryFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(AmqpApplication.class)
class AmqpApplicationTestConfiguration {

    @Bean
    public ConnectionFactory connectionFactory() {
        return new CachingConnectionFactory(MockConnectionFactoryFactory.build());
    }
}
