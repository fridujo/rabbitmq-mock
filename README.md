# RabbitMQ-mock
[![Build Status](https://travis-ci.com/fridujo/rabbitmq-mock.svg?branch=master)](https://travis-ci.com/fridujo/rabbitmq-mock)
[![Coverage Status](https://coveralls.io/repos/github/fridujo/rabbitmq-mock/badge.svg?branch=master)](https://coveralls.io/github/fridujo/rabbitmq-mock?branch=master)

Mock for RabbitMQ Java [amqp-client](https://github.com/rabbitmq/rabbitmq-java-client).

### Motivation

This project aims to emulate RabbitMQ behavior for test purposes, through:
* `com.rabbitmq.client.ConnectionFactory` with [`MockConnectionFactory`](src/main/java/com/github/fridujo/MockConnectionFactory.java)

## Example Use

Replace the use of `com.rabbitmq.client.ConnectionFactory` by [`MockConnectionFactory`](src/main/java/com/github/fridujo/MockConnectionFactory.java):

```java
ConnectionFactory factory = new MockConnectionFactory();
try (Connection conn = factory.newConnection()) {
    try (Channel channel = conn.createChannel()) {
        GetResponse response = channel.basicGet(queueName, autoAck);
        byte[] body = response.getBody();
        long deliveryTag = response.getEnvelope().getDeliveryTag();

        // Do what you need with the body

        channel.basicAck(deliveryTag, false);
    }
}
```

## Getting Started

### Building from Source

You need [JDK-8](http://jdk.java.net/8/) to build RabbitMQ-Mock. The project can be built with Maven using the following command.
```
mvn clean package
```

All integration tests can be launched with Maven using the following command.
```
mvn clean test
```

Since Maven has incremental build support, you can usually omit executing the clean goal.

### Installing in the Local Maven Repository

The project can be installed in a local Maven Repository for usage in other projects via the following command.
```
mvn clean install
```
