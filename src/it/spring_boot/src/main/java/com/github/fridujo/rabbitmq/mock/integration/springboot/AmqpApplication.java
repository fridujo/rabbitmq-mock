package com.github.fridujo.rabbitmq.mock.integration.springboot;

import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.io.UncheckedIOException;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class AmqpApplication {

    public final static String QUEUE_NAME = "spring-boot";

    public static void main(String[] args) throws InterruptedException {
        try (ConfigurableApplicationContext context = SpringApplication.run(AmqpApplication.class, args)) {
            rawConfiguration(context.getBean(ConnectionFactory.class));

            context.getBean(Sender.class).send();
            Receiver receiver = context.getBean(Receiver.class);
            while (receiver.getMessages().isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(100L);
            }
        }
    }

    private static void rawConfiguration(ConnectionFactory connectionFactory) {
        try {
            // Connection & Channel may not yet implement AutoCloseable
            Connection connection = connectionFactory.createConnection();
            Channel channel = connection.createChannel(false);
            channel.exchangeDeclare("xyz", "direct", true);
        } catch(IOException e) {
            throw new UncheckedIOException("Failed to declare an Exchange using Channel directly", e);
        }
    }

    @Bean
    public Queue queue() {
        return new Queue(QUEUE_NAME, false);
    }

    @Bean
    public TopicExchange exchange() {
        return new TopicExchange("spring-boot-exchange");
    }

    @Bean
    public Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(QUEUE_NAME);
    }

    @Bean
    public SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
                                                    MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(QUEUE_NAME);
        container.setMessageListener(listenerAdapter);
        return container;
    }

    @Bean
    public MessageListenerAdapter listenerAdapter(Receiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }
}
