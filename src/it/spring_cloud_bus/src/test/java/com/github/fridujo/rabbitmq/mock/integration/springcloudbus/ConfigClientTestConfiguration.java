package com.github.fridujo.rabbitmq.mock.integration.springcloudbus;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.bootstrap.config.PropertySourceBootstrapConfiguration;
import org.springframework.cloud.config.client.ConfigServicePropertySourceLocator;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import com.github.fridujo.rabbitmq.mock.compatibility.MockConnectionFactoryFactory;

/**
 * Order here is very important, this initializer must kick-in <u><b>before</b></u>
 * {@link PropertySourceBootstrapConfiguration} otherwise the {@link RestTemplate} mock
 * will be set after properties initialization.
 * <br>
 * This {@link ApplicationContextInitializer} is declared in META-INF/spring.factories to avoid losing it when context is refreshed.
 * <br>
 * Indeed {@link ApplicationContextInitializer} declared through
 * {@link org.springframework.test.context.ContextConfiguration} are lost if the application receives a
 * {@link org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent}.
 */
@Configuration
@Import(ConfigClient.class)
@Order(Ordered.HIGHEST_PRECEDENCE + 9)
public class ConfigClientTestConfiguration implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Bean
    public ConnectionFactory connectionFactory() {
        return new CachingConnectionFactory(MockConnectionFactoryFactory.build());
    }

    @Bean
    public ContextRefreshLock contextRefreshLock() {
        return new ContextRefreshLock();
    }

    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
        RestTemplate configServerClient = new RestTemplate();
        MockRestServiceServer mockConfigServer = MockRestServiceServer.bindTo(configServerClient).build();
        configServerClient.getInterceptors().add(0, new LoggingHttpInterceptor());
        // {@code putIfAbsent} here is important as we do not want to override values when context is refreshed
        ConfigServerValues configServerValues = new ConfigServerValues(mockConfigServer)
            .putIfAbsent(ConfigClient.USER_ROLE_KEY, "admin");

        ConfigurableListableBeanFactory beanFactory = configurableApplicationContext.getBeanFactory();

        try {
            beanFactory
                .getBean(ConfigServicePropertySourceLocator.class)
                .setRestTemplate(configServerClient);

            beanFactory.registerSingleton("configServerClient", configServerClient);
            beanFactory.registerSingleton("mockConfigServer", mockConfigServer);
            beanFactory.registerSingleton("configServerValues", configServerValues);

        } catch (NoSuchBeanDefinitionException e) {
            // too soon, ConfigServicePropertySourceLocator is not defined
        }
    }
}
