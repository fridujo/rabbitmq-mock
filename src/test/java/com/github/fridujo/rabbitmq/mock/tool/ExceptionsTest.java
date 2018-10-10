package com.github.fridujo.rabbitmq.mock.tool;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExceptionsTest {

    @Test
    void runAndEatExceptions_does_not_throw() throws Exception {
        Exceptions.ThrowingRunnable runnable = Mockito.mock(Exceptions.ThrowingRunnable.class);
        doThrow(new Exception("test")).when(runnable).run();

        Exceptions.runAndEatExceptions(runnable);

        verify(runnable, times(1)).run();
    }

    @Test
    void runAndTransformExceptions_throws_a_mapped_exception() throws Exception {
        Exceptions.ThrowingRunnable runnable = Mockito.mock(Exceptions.ThrowingRunnable.class);
        doThrow(new Exception("test")).when(runnable).run();

        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(() -> {
                Exceptions.runAndTransformExceptions(
                    runnable,
                    e -> new IllegalStateException("transform test", e)
                );
            })
            .withMessage("transform test")
            .withCauseExactlyInstanceOf(Exception.class);
    }
}
