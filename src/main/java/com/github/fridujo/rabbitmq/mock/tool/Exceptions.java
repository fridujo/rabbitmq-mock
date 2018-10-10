package com.github.fridujo.rabbitmq.mock.tool;

import java.util.function.Function;

public class Exceptions {

    public static void runAndEatExceptions(ThrowingRunnable throwingRunnable) {
        try {
            throwingRunnable.run();
        } catch (Exception unused) {
            // we are not interested in this error
        }
    }

    public static <T extends Exception> void runAndTransformExceptions(ThrowingRunnable throwingRunnable,
                                                                       Function<Exception, T> exceptionMapper) throws T {
        try {
            throwingRunnable.run();
        } catch (Exception original) {
            throw exceptionMapper.apply(original);
        }
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
