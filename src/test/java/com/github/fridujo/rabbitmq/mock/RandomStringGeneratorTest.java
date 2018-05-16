package com.github.fridujo.rabbitmq.mock;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RandomStringGeneratorTest {

    @Test
    void nominal_generation() {
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator("test-", "AB47", 5);

        assertThat(randomStringGenerator.generate()).matches("test-[AB47]{5}");
    }
}
