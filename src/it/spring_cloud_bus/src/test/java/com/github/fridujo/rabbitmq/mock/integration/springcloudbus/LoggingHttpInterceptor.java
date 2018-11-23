package com.github.fridujo.rabbitmq.mock.integration.springcloudbus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

public class LoggingHttpInterceptor implements ClientHttpRequestInterceptor {

    private final Logger logger = LoggerFactory.getLogger(LoggingHttpInterceptor.class);

    @Override
    public ClientHttpResponse intercept(HttpRequest request,
                                        byte[] body,
                                        ClientHttpRequestExecution execution) throws IOException {
        ClientHttpResponse response = execution.execute(request, body);
        String responseBody = toString((ByteArrayInputStream) response.getBody());
        logger.info("Config server received request " +
            request.getMethodValue() + " " + request.getURI() +
            " and responded:\n" + responseBody);
        return response;
    }

    private String toString(ByteArrayInputStream in) {
        int n = in.available();
        byte[] bytes = new byte[n];
        in.read(bytes, 0, n);
        in.reset();
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
