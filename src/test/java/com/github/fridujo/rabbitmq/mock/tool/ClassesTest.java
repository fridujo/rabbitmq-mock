package com.github.fridujo.rabbitmq.mock.tool;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClassesTest {

    @Test
    void existing_classes_detection() {
        assertThat(Classes.missingClass(this.getClass().getClassLoader(), "java.lang.String")).isFalse();
    }

    @Test
    void missing_classes_detection() {
        assertThat(Classes.missingClass(this.getClass().getClassLoader(), "com.DoesNotExists")).isTrue();
    }
}
