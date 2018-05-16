package com.github.fridujo.rabbitmq.mock;

import java.util.Random;
import java.util.stream.Collectors;

class RandomStringGenerator {

    private final String prefix;
    private final char[] availableCharacters;
    private final int length;

    RandomStringGenerator(String prefix, String availableCharacters, int length) {
        this.prefix = prefix;
        this.availableCharacters = availableCharacters.toCharArray();
        this.length = length;
    }

    String generate() {
        return prefix + new Random().ints(length, 0, availableCharacters.length)
            .mapToObj(i -> availableCharacters[i])
            .map(String::valueOf)
            .collect(Collectors.joining());
    }
}
