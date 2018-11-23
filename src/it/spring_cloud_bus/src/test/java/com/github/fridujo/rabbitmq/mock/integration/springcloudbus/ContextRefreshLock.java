package com.github.fridujo.rabbitmq.mock.integration.springcloudbus;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

public class ContextRefreshLock implements ApplicationListener<ApplicationEvent> {
    private final Logger logger = LoggerFactory.getLogger(ContextRefreshLock.class);
    private final Semaphore semaphore = new Semaphore(0);

    public void reset() {
        semaphore.drainPermits();
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof RefreshScopeRefreshedEvent) {
            logger.info("Context refreshed !");
            semaphore.release();
        } else {
            logger.info("Notified of " + event);
        }
    }

    public void acquire() throws InterruptedException, TimeoutException {
        if(!semaphore.tryAcquire(15L, TimeUnit.SECONDS)) {
            throw new TimeoutException("Lock not acquired withing 15 sec");
        }
    }
}
