package com.github.fridujo.rabbitmq.mock.tool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class RestartableExecutorServiceTest {

    @Test
    void all_calls_delegates() throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService delegate = mock(ExecutorService.class);
        ExecutorService executorService = new RestartableExecutorService(() -> delegate);

        executorService.shutdown();
        verify(delegate, atLeastOnce()).shutdown();

        executorService.shutdownNow();
        verify(delegate, atLeastOnce()).shutdownNow();

        executorService.isShutdown();
        verify(delegate, atLeastOnce()).isShutdown();

        executorService.isTerminated();
        verify(delegate, atLeastOnce()).isTerminated();

        executorService.awaitTermination(3L, TimeUnit.SECONDS);
        verify(delegate, atLeastOnce()).awaitTermination(3L, TimeUnit.SECONDS);

        executorService.submit(mock(Callable.class));
        verify(delegate, atLeastOnce()).submit(any(Callable.class));

        executorService.submit(mock(Runnable.class), new Object());
        verify(delegate, atLeastOnce()).submit(any(Runnable.class), any());

        executorService.submit(mock(Runnable.class));
        verify(delegate, atLeastOnce()).submit(any(Runnable.class));

        executorService.invokeAll(Collections.<Callable<Object>>singletonList(mock(Callable.class)));
        verify(delegate, atLeastOnce()).invokeAll(any());

        executorService.invokeAll(Collections.<Callable<Object>>singletonList(mock(Callable.class)), 7L, TimeUnit.MILLISECONDS);
        verify(delegate, atLeastOnce()).invokeAll(any(), eq(7L), eq(TimeUnit.MILLISECONDS));

        executorService.invokeAny(Collections.<Callable<Object>>singletonList(mock(Callable.class)));
        verify(delegate, atLeastOnce()).invokeAny(any());

        executorService.invokeAny(Collections.<Callable<Object>>singletonList(mock(Callable.class)), 7L, TimeUnit.MILLISECONDS);
        verify(delegate, atLeastOnce()).invokeAny(any(), eq(7L), eq(TimeUnit.MILLISECONDS));

        executorService.execute(mock(Runnable.class));
        verify(delegate, atLeastOnce()).execute(any());
    }

    @Test
    void restart_creates_a_new_delegate_from_factory() {
        AtomicInteger counter = new AtomicInteger();
        RestartableExecutorService executorService = new RestartableExecutorService(() -> mock(ExecutorService.class, "mock" + counter.incrementAndGet()));

        assertThat(executorService.getDelegate()).hasToString("mock1");

        executorService.restart();

        assertThat(executorService.getDelegate()).hasToString("mock2");
    }
}
