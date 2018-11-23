package com.github.fridujo.rabbitmq.mock.integration.springcloudbus;

import static org.hjson.JsonValue.readHjson;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.hjson.Stringify;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;

public class ConfigServerValues {
    /**
     * {@code static} here is important as Spring will recreate beans when context is refreshed,
     * whereas we do not want to loose configured data from one context to another.
     */
    private static final Map<String, String> SOURCE = new LinkedHashMap<>();
    private final MockRestServiceServer mockConfigServer;

    public ConfigServerValues(MockRestServiceServer mockConfigServer) {
        this.mockConfigServer = mockConfigServer;
    }

    public ConfigServerValues put(String key, String value) {
        SOURCE.put(key, value);
        configure(mockConfigServer);
        return this;
    }

    public ConfigServerValues putIfAbsent(String key, String value) {
        if (!SOURCE.containsKey(key)) {
            SOURCE.put(key, value);
        }
        configure(mockConfigServer);
        return this;
    }

    private void configure(MockRestServiceServer mockConfigServer) {
        String config = "{\n"
            + "  name: 'config-server-env'\n"
            + "  propertySources: [{\n"
            + "    name: 'config-server-test-source'\n"
            + "    source: {\n"
            + sourceAsHjson()
            + "    }\n"
            + "  }]\n"
            + "}";
        mockConfigServer.reset();
        mockConfigServer.expect(ExpectedCount.manyTimes(), requestTo(CoreMatchers.endsWith("/application/default")))
            .andRespond(withSuccess(
                readHjson(config).toString(Stringify.FORMATTED),
                MediaType.APPLICATION_JSON_UTF8
            ));
    }

    private String sourceAsHjson() {
        return SOURCE
            .entrySet()
            .stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining("\n      ", "      ", "\n"));
    }
}
