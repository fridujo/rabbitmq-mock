package com.github.fridujo.rabbitmq.mock.tool;

import org.mockito.ArgumentMatchers;

public abstract class SafeArgumentMatchers {
    private SafeArgumentMatchers() {
    }

    public static String eq(String expected) {
        ArgumentMatchers.eq(expected);
        return "";
    }
}
