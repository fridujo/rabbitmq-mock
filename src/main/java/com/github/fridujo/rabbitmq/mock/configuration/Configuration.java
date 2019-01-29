package com.github.fridujo.rabbitmq.mock.configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import com.github.fridujo.rabbitmq.mock.exchange.MockExchangeCreator;

public class Configuration {
    private Map<String, MockExchangeCreator> additionalExchangeCreatorsByType = new LinkedHashMap<>();

    public void registerAdditionalExchangeCreator(MockExchangeCreator mockExchangeCreator) {
        additionalExchangeCreatorsByType.put(mockExchangeCreator.getType(), mockExchangeCreator);
    }

    public MockExchangeCreator getAdditionalExchangeByType(String type) {
        return additionalExchangeCreatorsByType.get(type);
    }
    
    public boolean isAdditionalExchangeRegisteredFor(String type) {
        return additionalExchangeCreatorsByType.containsKey(type);
    }
}
