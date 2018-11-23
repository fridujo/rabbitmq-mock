package com.github.fridujo.rabbitmq.mock.integration.springcloudbus;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = ConfigClientTestConfiguration.class)
@SpringBootTest
@AutoConfigureMockMvc
class ConfigClientTest {

    private final Logger logger = LoggerFactory.getLogger(ConfigClientTest.class);
    private final ConfigServerValues configServerValues;
    private final MockMvc mockMvc;
    private final RabbitTemplate rabbitTemplate;
    private final ContextRefreshLock contextRefreshLock;
    private final AtomicInteger userSequence = new AtomicInteger();

    ConfigClientTest(@Autowired ConfigServerValues configServerValues,
                     @Autowired MockMvc mockMvc,
                     @Autowired RabbitTemplate rabbitTemplate,
                     @Autowired ContextRefreshLock contextRefreshLock) {
        this.configServerValues = configServerValues;
        this.mockMvc = mockMvc;
        this.rabbitTemplate = rabbitTemplate;
        this.contextRefreshLock = contextRefreshLock;
    }

    @Test
    void trigger_refresh() throws Exception {
        assertThatConfiguredValueIs("admin");

        configServerValues.put(ConfigClient.USER_ROLE_KEY, "dev");

        assertThatConfiguredValueIs("admin");

        contextRefreshLock.reset();

        RefreshRemoteApplicationEvent event = new RefreshRemoteApplicationEvent(this, "test-id", null);
        logger.info("\n\n <<<refresh event sent through RabbitMQ>>>\n\n");
        rabbitTemplate.convertAndSend("springCloudBus", "unused", event);

        contextRefreshLock.acquire();
        assertThatConfiguredValueIs("dev");
    }

    private void assertThatConfiguredValueIs(final String role) throws Exception {
        String userName = "user-" + userSequence.incrementAndGet();
        mockMvc.perform(MockMvcRequestBuilders.get("/whoami/" + userName))
            .andExpect(status().isOk())
            .andExpect(content().string("Hello! You're " + userName + " and you'll become a(n) " + role + "...\n"));
    }
}
