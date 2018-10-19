# RabbitMQ-mock
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/eddfd3e0ca8c44379becf68efd6524af)](https://app.codacy.com/app/ledoyen/rabbitmq-mock?utm_source=github.com&utm_medium=referral&utm_content=fridujo/rabbitmq-mock&utm_campaign=badger)
[![Build Status](https://travis-ci.com/fridujo/rabbitmq-mock.svg?branch=master)](https://travis-ci.com/fridujo/rabbitmq-mock)
[![Coverage Status](https://codecov.io/gh/fridujo/rabbitmq-mock/branch/master/graph/badge.svg)](https://codecov.io/gh/fridujo/rabbitmq-mock/)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.fridujo/rabbitmq-mock.svg)](https://search.maven.org/#search|ga|1|a:"rabbitmq-mock")
[![JitPack](https://jitpack.io/v/fridujo/rabbitmq-mock.svg)](https://jitpack.io/#fridujo/rabbitmq-mock)
[![License](https://img.shields.io/github/license/fridujo/spring-automocker.svg)](https://opensource.org/licenses/Apache-2.0)

Mock for RabbitMQ Java [amqp-client](https://github.com/rabbitmq/rabbitmq-java-client).

> Compatible with versions **4.0.0** to **5.4.3** of [**com.rabbitmq:amqp-client**](https://github.com/rabbitmq/rabbitmq-java-client)

> Compatible with versions **3.6.3** to **4.0.0** with the [`com.github.fridujo.rabbitmq.mock.compatibility` package](src/main/java/com/github/fridujo/rabbitmq/mock/compatibility/MockConnectionFactoryFactory.java).

### Motivation

This project aims to emulate RabbitMQ behavior for test purposes, through:
* `com.rabbitmq.client.ConnectionFactory` with [`MockConnectionFactory`](src/main/java/com/github/fridujo/rabbitmq/mock/MockConnectionFactory.java)

## Example Use

### Without Spring
Replace the use of `com.rabbitmq.client.ConnectionFactory` by [`MockConnectionFactory`](src/main/java/com/github/fridujo/rabbitmq/mock/MockConnectionFactory.java)

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

More details in [integration-test](src/test/java/com/github/fridujo/rabbitmq/mock/IntegrationTest.java)

### With Spring
Change underlying RabbitMQ ConnectionFactory by [`MockConnectionFactory`](src/main/java/com/github/fridujo/rabbitmq/mock/MockConnectionFactory.java)

```java

@Configuration
@Import(AppConfiguration.class)
class TestConfiguration {
    @Bean
    ConnectionFactory connectionFactory() {
        return new CachingConnectionFactory(new MockConnectionFactory());
    }
}
```

More details in [integration-test](src/test/java/com/github/fridujo/rabbitmq/mock/spring/SpringIntegrationTest.java)

## Getting Started

### Maven
Add the following dependency to your **pom.xml**
```xml
<dependency>
    <groupId>com.github.fridujo</groupId>
    <artifactId>rabbitmq-mock</artifactId>
    <version>1.0.7</version>
    <scope>test</scope>
</dependency>
```

### Gradle
Add the following dependency to your **build.gradle**
```groovy
repositories {
	mavenCentral()
}

// ...

dependencies {
	// ...
	testCompile('com.github.fridujo:rabbitmq-mock:1.0.7')
	// ...
}
```

### Building from Source

You need [JDK-8](http://jdk.java.net/8/) to build RabbitMQ-Mock. The project can be built with Maven using the following command.
```
mvn clean package
```

Tests are split in:

* **unit tests** covering features and borderline cases: `mvn test`
* **integration tests**, seatbelts for integration with Spring and Spring-Boot. These tests use the **maven-invoker-plugin** to launch the same project (in [src/it/spring_boot](src/it/spring_boot)) with different versions of the dependencies: `mvn integration-test`
* **mutation tests**, to help understand what is missing in test assertions: `mvn org.pitest:pitest-maven:mutationCoverage`


### Installing in the Local Maven Repository

The project can be installed in a local Maven Repository for usage in other projects via the following command.
```
mvn clean install
```

### Using the latest SNAPSHOT

The master of the project pushes SNAPSHOTs in Sonatype's repo.

To use the latest master build add Sonatype OSS snapshot repository, for Maven:
```
<repositories>
    ...
    <repository>
        <id>sonatype-oss-spanshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>
```

For Gradle:
```groovy
repositories {
    // ...
    maven {
        url "https://oss.sonatype.org/content/repositories/snapshots"
    }
}
