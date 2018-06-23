package com.github.fridujo.rabbitmq.mock.integration.springboot;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class Receiver {

    private final List<String> messages = new ArrayList<>();

    public void receiveMessage(String message) {
        System.out.println("Received <" + message + ">");
        this.messages.add(message);
    }

    public List<String> getMessages() {
        return messages;
    }
}
