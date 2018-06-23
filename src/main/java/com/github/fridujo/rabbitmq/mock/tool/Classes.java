package com.github.fridujo.rabbitmq.mock.tool;

public abstract class Classes {
    private Classes() {
    }

    public static boolean missingClass(ClassLoader classLoader, String className) {
        try {
            classLoader.loadClass(className);
            return false;
        } catch (ClassNotFoundException e) {
            return true;
        }
    }
}
