package com.github.fridujo.rabbitmq.mock.configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import com.github.fridujo.rabbitmq.mock.exchange.MockExchangeCreator;
import com.github.fridujo.rabbitmq.mock.exchange.TypedMockExchangeCreator;

public class Configuration {
    private Map<String, MockExchangeCreator> additionalExchangeCreatorsByType = new LinkedHashMap<>();

    public Configuration registerAdditionalExchangeCreator(TypedMockExchangeCreator mockExchangeCreator) {
        additionalExchangeCreatorsByType.put(mockExchangeCreator.getType(), mockExchangeCreator);
        return this;
    }

    public MockExchangeCreator getAdditionalExchangeByType(String type) {
        return additionalExchangeCreatorsByType.get(type);
    }
    
    public boolean isAdditionalExchangeRegisteredFor(String type) {
        return additionalExchangeCreatorsByType.containsKey(type);
    }
}
